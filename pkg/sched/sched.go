package sched

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/messenger"
	"github.com/mesos/mesos-go/upid"
	"golang.org/x/net/context"
)

// state machine implementation of a scheduler driver
type schedulerDriver struct {
	ops        chan opRequest
	callbacks  chan *callback
	out        messenger.Messenger // send msg to mesos
	status     statusType
	statusLock sync.RWMutex // provide the stub with a reliable view of status
	req        opRequest    // most recent
	master     *mesos.MasterInfo
	cancel     func() // cancels driver context, the driver will terminate
	cancelOnce sync.Once
	cache      *schedCache // concurrent cache
	stub       SchedulerDriver
	sched      Scheduler
	connected  bool
	dispatch   func(ctx context.Context, cb *callback) // pluggable, via config.dispatch
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
		callbacks: make(chan *callback, 1024),
		out:       out,
		status:    NOT_STARTED,
		cache:     newSchedCache(),
		framework: config.framework,
	}
	if config.dispatch == nil {
		driver.dispatch = driver.defaultDispatch()
	} else {
		driver.dispatch = config.dispatch
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
		status: func() mesos.Status {
			driver.statusLock.RLock()
			defer driver.statusLock.RUnlock()
			return mesos.Status(driver.status)
		},
	}
	driver.stub, driver.sched = stub, config.sched
	go func() {
		go driver.opsLoop(ctx)
		select {
		case <-ctx.Done():
			log.Infoln("shutdown: stopping internal messenger")
			out.Stop()
		case <-out.Done():
			log.Infoln("detected stopped messenger, initiating driver shutdown")
			driver.cancel()
		}
	}()
	return
}

func (driver *schedulerDriver) defaultDispatch() func(context.Context, *callback) {
	return func(ctx context.Context, cb *callback) {
		select {
		case <-ctx.Done():
			log.Warningf("discarding callback because driver is shutting down: %+v", cb)
		case driver.callbacks <- cb:
		}
	}
}

// serialize ops processing via state machine transitions. this is the ONLY func that
// should change driver.status.
func (driver *schedulerDriver) opsLoop(ctx context.Context) {
	defer func() {
		driver.status = ABORTED
		driver.cancel()
	}()
	for state := initStateFn; driver.status != ABORTED; {
		select {
		case <-ctx.Done():
			log.V(1).Infoln("exiting ops loop, driver is shutting down")
			return

		case cb, ok := <-driver.callbacks:
			if !ok {
				panic("programming error, callbacks channel closed")
			}
			driver.req = opRequest{opcode: handleCallbackOp, cb: cb}

		case req, ok := <-driver.ops:
			if !ok {
				panic("programming error, ops channel closed")
			}
			driver.req = req
		}

		//TODO(jdef) could impose a global timeout for state transitions here...
		newstate, resp := func() (stateFn, opResponse) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel() // state transition is complete
			return state(ctx, driver)
		}()

		if newstate == nil {
			// no state fn change? then no status change allowed..
			continue
		}
		state = newstate

		switch st := resp.status; st {
		case NO_RESPONSE_REQUIRED:
			if driver.req.opcode < __internalOps {
				// programming / algorithm error
				panic(fmt.Sprintf("non-internal op generated no response: req='%+v' resp='%+v'", driver.req, resp))
			}
		default:
			// need to synchronize here so that the stub has a reliable view
			driver.statusLock.Lock()
			driver.status = st
			driver.statusLock.Unlock()

			select {
			case <-ctx.Done():
				log.Warningf("discarding op response because driver is shutting down: %+v", resp)
				return
			case driver.req.out <- resp:
				// noop
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
func disconnectedStateFn(ctx context.Context, d *schedulerDriver) (fn stateFn, resp opResponse) {
	d.status.Check(RUNNING)
	resp.status = d.status

	switch d.req.opcode {
	case abortOp:
		fn, resp = d.doAbort(ctx, nil)
	case stopOp:
		fn, resp = d.doStop(ctx)
	case handleCallbackOp:
		resp.status = NO_RESPONSE_REQUIRED
		if opcode := d.req.cb.opcode; opcode == disconnectedCb || opcode > __internalCbs {
			fn = d.doCallback(ctx, d.req.cb)
			return
		}
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

func connectedStateFn(ctx context.Context, d *schedulerDriver) (fn stateFn, resp opResponse) {
	panic("connected state not yet implemented")
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
		fn = s.handleDisconnected(ctx)
	case masterDetectedCb:
		fn = s.handleDetected(ctx, cb.msg)
	case authCompletedCb:
		fn = s.handleAuthCompleted(ctx, cb.msg)
	case registeredCb:
		fn = s.handleRegistered(ctx, cb.msg)
	case reregisteredCb:
		fn = s.handleReregistered(ctx, cb.msg)
	//TODO(jdef) other cases here....
	default:
		panic(fmt.Sprintf("unrecognized callback opcode: %v", opcode))
	}
	return
}

func (s *schedulerDriver) doStop(ctx context.Context) (stateFn, opResponse) {
	panic("stop not yet implemented")
}

func (s *schedulerDriver) doStart(ctx context.Context) (stateFn, opResponse) {
	return disconnectedStateFn, opResponse{status: RUNNING}
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

func (s *schedulerDriver) handleDisconnected(ctx context.Context) (fn stateFn) {
	s.connected = false
	s.sched.Disconnected(s.stub)
	fn = s.postDetectionStateFn() // enter either authenticating or registering state
	return
}

func (s *schedulerDriver) handleDetected(ctx context.Context, pbmsg proto.Message) (fn stateFn) {
	if !reflect.DeepEqual(s.master, pbmsg) {
		if pbmsg == nil {
			s.master = nil
		} else {
			// new master in town..
			s.master = pbmsg.(*mesos.MasterInfo)
			if !s.connected {
				fn = s.postDetectionStateFn()
				return
			}
		}
		// possibilities:
		// (a) we lost a leading master (we have or may not have been connected)
		// (b) there is a new leading master, so signal a disconnect from the old one
		fn = s.handleDisconnected(ctx)
	}
	// else noop, master didn't really change
	return
}

func (s *schedulerDriver) handleAuthCompleted(ctx context.Context, pbmsg proto.Message) (fn stateFn) {
	panic("not yet implemented")
}

func (s *schedulerDriver) handleRegistered(ctx context.Context, pbmsg proto.Message) stateFn {
	msg := pbmsg.(*mesos.FrameworkRegisteredMessage)
	s.framework.Id = msg.GetFrameworkId() // generated by master.
	s.connected = true
	s.sched.Registered(s.stub, s.framework.Id, msg.GetMasterInfo())
	return connectedStateFn
}

func (s *schedulerDriver) handleReregistered(ctx context.Context, pbmsg proto.Message) stateFn {
	msg := pbmsg.(*mesos.FrameworkRegisteredMessage)
	//TODO(jdef) assert frameworkId from masterInfo matches what we have on record?
	s.connected = true
	s.sched.Reregistered(s.stub, msg.GetMasterInfo())
	return connectedStateFn
}
