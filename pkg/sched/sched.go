package sched

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/gogo/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/messenger"
	"github.com/mesos/mesos-go/upid"
	"golang.org/x/net/context"
)

// state machine implementation of a scheduler driver
type schedulerDriver struct {
	ops        chan opRequest
	out        messenger.Messenger // send msg to mesos
	status     statusType
	req        opRequest // most recent
	master     *mesos.MasterInfo
	cancel     func() // cancels context, async loops will terminate
	cancelOnce sync.Once
	cache      *schedCache // concurrent cache
	stub       SchedulerDriver
	sched      Scheduler
	registered bool
	dispatch   func(ctx context.Context, cb *callback) // pluggable, dispatch an invocation against Scheduler interface
	framework  *mesos.FrameworkInfo
}

// returns a NOT_STARTED scheduler driver, or else error.
func New(config driverConfig) (stub SchedulerDriver, err error) {
	//TODO(jdef) expand on validation
	if config.framework == nil {
		err = frameworkRequiredError
		return
	}

	var out messenger.Messenger
	out, err = config.messenger()
	if err != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	driver := &schedulerDriver{
		ops:       make(chan opRequest, 1024),
		out:       out,
		status:    NOT_STARTED,
		cache:     newSchedCache(),
		framework: config.framework,
	}
	driver.dispatch = func(ctx context.Context, cb *callback) {
		select {
		case <-ctx.Done():
		case driver.ops <- opRequest{opcode: handleCallbackOp, cb: cb}:
		}
	}
	// forward messages from messenger to the callback dispatcher..
	for opcode, msg := range map[callbackType]proto.Message{
		registeredCb:       &mesos.FrameworkRegisteredMessage{},
		reregisteredCb:     &mesos.FrameworkReregisteredMessage{},
		offerRescindedCb:   &mesos.RescindResourceOfferMessage{},
		resourceOffersCb:   &mesos.ResourceOffersMessage{},
		statusUpdateCb:     &mesos.StatusUpdateMessage{},
		frameworkMessageCb: &mesos.ExecutorToFrameworkMessage{},
		slaveLostCb:        &mesos.LostSlaveMessage{},
		frameworkErrorCb:   &mesos.FrameworkErrorMessage{},
		//...
	} {
		out.Install(func(from *upid.UPID, msg proto.Message) {
			driver.dispatch(ctx, &callback{opcode: opcode, msg: msg, from: from})
		}, msg)
	}
	//TODO(jdef) not sure we really need to wrap this... check context docs
	driver.cancel = func() {
		driver.cancelOnce.Do(cancel)
	}
	stub = &schedulerDriverStub{
		ops:   driver.ops,
		cache: driver.cache,
		done:  ctx.Done(),
	}
	driver.stub, driver.sched = stub, config.sched
	go func() {
		go driver.opsLoop(ctx)
		select {
		case <-ctx.Done():
			out.Stop()
		case <-out.Done():
			driver.cancel()
		}
	}()
	return
}

// serialize ops processing via state machine transitions
func (driver *schedulerDriver) opsLoop(ctx context.Context) {
	defer func() {
		driver.status = ABORTED
		driver.cancel()
	}()
	for state := initStateFn; driver.status != ABORTED; {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-driver.ops:
			if !ok {
				panic("programming error, ops channel closed")
			}

			driver.req = req

			//TODO(jdef) could impose a global timeout for state transitions here...
			newstate, resp := func() (stateFn, opResponse) {
				ctx, cancel := context.WithCancel(ctx)
				defer cancel() // state transition is complete
				return state(ctx, driver)
			}()

			if newstate != nil {
				state = newstate
			}

			switch st := resp.status; st {
			case NO_RESPONSE_REQUIRED:
				if req.opcode < __internalOps {
					panic(fmt.Sprintf("non-internal op generated no response: req='%+v' resp='%+v'", req, resp))
				}
			default:
				driver.status = st
				select {
				case <-ctx.Done():
					return
				case req.out <- resp:
					// noop
				}
			}
		}
	}
}

// driver is not running in this state.
// allowable ops are: start, abort, run
func initStateFn(ctx context.Context, d *schedulerDriver) (fn stateFn, resp opResponse) {
	d.status.Check(NOT_STARTED)
	resp.status = d.status

	switch d.req.opcode {
	case startOp, runOp:
		fn, resp = d.doStart(ctx)
	case abortOp:
		fn, resp = d.doAbort(ctx, nil)
	default:
		resp.err = d.status.Illegal(NotRunning)
	}
	return
}

// driver is running and listening for a signal from the master detector.
func detectingStateFn(ctx context.Context, d *schedulerDriver) (fn stateFn, resp opResponse) {
	d.status.Check(RUNNING)
	resp.status = d.status

	switch d.req.opcode {
	case masterDetectedOp:
		fn, resp = d.doDetected(ctx)
	case abortOp:
		fn, resp = d.doAbort(ctx, nil)
	case stopOp:
		fn, resp = d.doStop(ctx)
	case authCompletedOp:
		//TODO(jdef) older authentication attempt finished, but too late...
		fn, resp = d.doAuthCompleted(ctx)
	case handleCallbackOp:
		if d.req.cb.opcode == disconnectedCb {
			fn = d.doCallback(ctx, d.req.cb)
			return
		}
		fallthrough
	default:
		resp.err = d.status.Illegal(NotConnected)
	}
	return
}

func authenticatingStateFn(ctx context.Context, d *schedulerDriver) (fn stateFn, resp opResponse) {
	panic("authenticating state not yet implemented")
}

func registeringStateFn(ctx context.Context, d *schedulerDriver) (fn stateFn, resp opResponse) {
	panic("registering state not yet implemented")
}

func abortedStateFn(ctx context.Context, d *schedulerDriver) (fn stateFn, resp opResponse) {
	panic("aborted state not yet implemented")
}

func (s *schedulerDriver) doCallback(ctx context.Context, cb *callback) (fn stateFn) {
	if cb == nil {
		//programming error
		panic("request's callback was nil")
	}
	switch opcode := cb.opcode; opcode {
	case disconnectedCb:
		s.registered = false
		fn = s.postDetectionStateFn()
		s.sched.Disconnected(s.stub)
	case registeredCb:
		s.handleRegistered(ctx, cb.msg)
	case reregisteredCb:
		s.handleReregistered(ctx, cb.msg)
	default:
		panic(fmt.Sprintf("unrecognized callback opcode: %v", opcode))
	}
	return
}

func (s *schedulerDriver) doStop(ctx context.Context) (stateFn, opResponse) {
	panic("stop not yet implemented")
}

func (s *schedulerDriver) doStart(ctx context.Context) (stateFn, opResponse) {
	return detectingStateFn, opResponse{status: RUNNING}
}

func (s *schedulerDriver) doDetected(ctx context.Context) (fn stateFn, resp opResponse) {
	resp.status = NO_RESPONSE_REQUIRED
	if !reflect.DeepEqual(s.master, s.req.msg) {
		if s.req.msg != nil && !s.registered {
			fn = s.postDetectionStateFn()
		} else {
			// possibilities:
			// (a) we lost a leading master (we have or may not have been registered)
			// (b) there is a new leading master, so signal a disconnect from the old one
			fn = s.doCallback(ctx, &callback{opcode: disconnectedCb})
		}
	} // else noop, master didn't really change
	return
}

func (s *schedulerDriver) shouldAuthenticate() bool {
	panic("not yet implemented")
}

func (s *schedulerDriver) postDetectionStateFn() stateFn {
	if s.shouldAuthenticate() {
		return authenticatingStateFn
	} else {
		return registeringStateFn
	}
}

func (s *schedulerDriver) doAbort(ctx context.Context, err error) (fn stateFn, resp opResponse) {
	switch s.status {
	// probably need to do different things here depending if we're running already
	}
	fn = abortedStateFn
	resp.status = ABORTED
	resp.err = err
	return
}

func (s *schedulerDriver) doAuthCompleted(ctx context.Context) (fn stateFn, resp opResponse) {
	panic("not yet implemented")
}

func (s *schedulerDriver) doSend(ctx context.Context, msg proto.Message, nextfn stateFn, resp opResponse) (stateFn, opResponse) {
	if err := s.out.Send(ctx, s.out.UPID(), msg); err != nil {
		return s.doAbort(ctx, err)
	}
	return nextfn, resp
}

//
// callback handlers, should forward callbacks to the Scheduler implementation.
// they're only ever invoked if the driver is an appropriate state.
//

func (s *schedulerDriver) handleRegistered(ctx context.Context, pbmsg proto.Message) {
	msg := pbmsg.(*mesos.FrameworkRegisteredMessage)
	frameworkId := msg.GetFrameworkId()

	s.framework.Id = frameworkId // generated by master.
	s.sched.Registered(s.stub, frameworkId, msg.GetMasterInfo())
}

func (s *schedulerDriver) handleReregistered(ctx context.Context, pbmsg proto.Message) {
	msg := pbmsg.(*mesos.FrameworkRegisteredMessage)
	frameworkId := msg.GetFrameworkId()

	s.framework.Id = frameworkId // generated by master.
	s.sched.Reregistered(s.stub, msg.GetMasterInfo())
}
