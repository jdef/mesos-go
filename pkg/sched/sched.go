package sched

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

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
	req        opRequest           // most recent
	master     *mesos.MasterInfo
	cancel     func() // cancels driver context, the driver will terminate
	cancelOnce sync.Once
	cache      *schedCache // concurrent cache
	stub       SchedulerDriver
	sched      Scheduler
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
	status := NOT_STARTED
	setStatus := func(s statusType) statusType {
		atomic.StoreInt32((*int32)(&status), int32(s))
		return s
	}
	stub = &schedulerDriverStub{
		ops:   driver.ops,
		cache: driver.cache,
		done:  ctx.Done(),
		status: func() mesos.Status {
			return mesos.Status(atomic.LoadInt32((*int32)(&status)))
		},
	}
	driver.stub, driver.sched = stub, config.sched
	go func() {
		go driver.opsLoop(ctx, status, setStatus)
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

// Serialize ops processing via state machine transitions. The callbacks and ops channels are
// processed with equal priority until either the ctx.Done channel closes or else the driver
// status becomes ABORTED. The initial state is determined from the status parameter. The state
// machine moves forward by invoking state functions (stateFn), interpreting the results by
// applying the following (exclusive) rules, in order:
//
// State functions that return:
//   - a different status will always advance to the initial stateFunc for that status
//   - the same status and a non-nil stateFunc will advance to the new stateFunc
//   - the same status and a nil stateFunc will not advance (and will be invoked for the next op)
//
func (driver *schedulerDriver) opsLoop(ctx context.Context, status statusType, setStatus func(statusType) statusType) {
	defer func() {
		setStatus(ABORTED)
		driver.cancel()
	}()
	for state := status.initialState(); status != ABORTED; {
		select {
		case <-ctx.Done():
			log.V(1).Infoln("exiting ops loop, driver is shutting down")
			return
		case cb, ok := <-driver.callbacks:
			if !ok {
				panic("programming error, callbacks channel closed")
			}
			driver.req = opRequest{opcode: handleCallbackOp, cb: cb} // nil .out is purposeful
		case req, ok := <-driver.ops:
			if !ok {
				panic("programming error, ops channel closed")
			} else if req.out == nil {
				panic("programming error, opRequest.out must not be nil")
			} else if driver.req.opcode > __internalOps {
				panic("programming error, internal opRequest not allowed on the ops channel")
			}
			driver.req = req
		}

		//TODO(jdef) could impose a global timeout for state transitions here...
		newstate, resp := func() (stateFn, opResponse) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel() // state transition is complete
			return state(ctx, driver)
		}()

		if resp.status != status {
			status = setStatus(resp.status)
			state = status.initialState()
		} else if newstate != nil {
			state = newstate
		}

		if driver.req.out != nil {
			select {
			case <-ctx.Done():
				log.Warningf("discarding op response because driver is shutting down: %+v", resp)
				return
			case driver.req.out <- resp: // noop
			}
		} else if resp.err != nil {
			log.Error(resp.err)
		}
	}
}

func (status statusType) initialState() (fn stateFn) {
	switch status {
	case NOT_STARTED:
		return status.initStateFn
	case RUNNING:
		return status.disconnectedStateFn
	case STOPPED:
		return status.stoppedStateFn
	case ABORTED:
		return status.abortedStateFn
	default:
		panic(fmt.Sprintln("unsupported status type: %v", status))
	}
}

// panic if the actual status doesn't match the expected status.
// should be used to guard against programming errors.
func (actual statusType) check(expected statusType) statusType {
	if actual != expected {
		panic(fmt.Sprintf("expected status %v instead of %v", expected, actual))
	}
	return actual
}

// driver is not running in this state.
// allowable ops are: start, abort, run
func (currentStatus statusType) initStateFn(ctx context.Context, d *schedulerDriver) (fn stateFn, resp opResponse) {
	resp.status = currentStatus.check(NOT_STARTED)

	switch d.req.opcode {
	case startOp, runOp:
		fn, resp = d.doStart(ctx)
	case abortOp:
		fn, resp = d.doAbort(ctx, nil)
	default:
		resp.err = currentStatus.Illegal(NotRunning)
	}
	return
}

// driver is running and listening for a signal from the master detector.
func (currentStatus statusType) disconnectedStateFn(ctx context.Context, d *schedulerDriver) (fn stateFn, resp opResponse) {
	resp.status = currentStatus.check(RUNNING)

	switch d.req.opcode {
	case abortOp:
		fn, resp = d.doAbort(ctx, nil)
	case stopOp:
		fn, resp = d.doStop(ctx, false)
	case handleCallbackOp:
		switch cb := d.req.cb; cb.opcode {
		case masterDetectedCb:
			fn = d.postDetectionStateFn()
			// ..and ignore every other callback while we're disconnected
		}
	default:
		resp.err = currentStatus.Illegal(NotConnected)
	}
	return
}

func (currentStatus statusType) authenticatingStateFn(ctx context.Context, d *schedulerDriver) (fn stateFn, resp opResponse) {
	panic("authenticating state not yet implemented")
}

func (currentStatus statusType) registeringStateFn(ctx context.Context, d *schedulerDriver) (fn stateFn, resp opResponse) {
	resp.status = currentStatus.check(RUNNING)

	switch d.req.opcode {
	case abortOp:
		fn, resp = d.doAbort(ctx, nil)
	case stopOp:
		fn, resp = d.doStop(ctx, false)
	case handleCallbackOp:
		switch cb := d.req.cb; cb.opcode {
		case masterDetectedCb:
			fn, _ = d.handleMasterChanged(ctx, cb.msg)
		case registeredCb:
			//TODO(jdef) handle registeredCb
		case reregisteredCb:
			//TODO(jdef) handle reregisteredCb
		}
	default:
		resp.err = currentStatus.Illegal(NotConnected)
	}
	return
}

func (currentStatus statusType) connectedStateFn(ctx context.Context, d *schedulerDriver) (fn stateFn, resp opResponse) {
	resp.status = currentStatus.check(RUNNING)

	switch d.req.opcode {
	case abortOp:
		fn, resp = d.doAbort(ctx, nil)
	case stopOp:
		fn, resp = d.doStop(ctx, true)
	case handleCallbackOp:
		switch cb := d.req.cb; cb.opcode {
		case disconnectedCb:
			d.sched.Disconnected(d.stub)
		case registeredCb, reregisteredCb, authCompletedCb:
			// ignore
		case masterDetectedCb:
			var masterLost bool
			if fn, masterLost = d.handleMasterChanged(ctx, cb.msg); masterLost {
				d.sched.Disconnected(d.stub)
			}
		default:
			fn = d.doCallback(ctx, cb)
		}
	default:
		resp.err = currentStatus.Illegal(NotConnected)
	}
	return
}

func (currentStatus statusType) abortedStateFn(ctx context.Context, d *schedulerDriver) (fn stateFn, resp opResponse) {
	panic("aborted state not yet implemented")
}

func (currentStatus statusType) stoppedStateFn(ctx context.Context, d *schedulerDriver) (fn stateFn, resp opResponse) {
	panic("stopped state not yet implemented")
}

func (s *schedulerDriver) doCallback(ctx context.Context, cb *callback) (fn stateFn) {
	if cb == nil {
		//programming error
		panic("request's callback was nil")
	} else if cb.opcode > __internalCbs {
		//programming error
		panic("expected state fn to handle internal cb")
	}

	switch cb.opcode {
	case registeredCb:
		fn = s.handleRegistered(ctx, cb.msg)
	case reregisteredCb:
		fn = s.handleReregistered(ctx, cb.msg)

	//TODO(jdef) other mesos callbacks here..

	default:
		panic(fmt.Sprintf("unrecognized mesos callback opcode: %v", cb.opcode))
	}
	return
}

func (s *schedulerDriver) doStop(ctx context.Context, connected bool) (stateFn, opResponse) {
	panic("stop not yet implemented")
}

func (s *schedulerDriver) doStart(ctx context.Context) (fn stateFn, resp opResponse) {
	resp.status = RUNNING
	return
}

func (s *schedulerDriver) shouldAuthenticate() bool {
	panic("not yet implemented")
}

func (s *schedulerDriver) postDetectionStateFn() stateFn {
	if s.shouldAuthenticate() {
		return RUNNING.authenticatingStateFn
	} else {
		return RUNNING.registeringStateFn
	}
}

func (s *schedulerDriver) doAbort(ctx context.Context, err error) (fn stateFn, resp opResponse) {
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

// master possibly changed. if so update our records and return both the next state fn & master status
func (d *schedulerDriver) handleMasterChanged(ctx context.Context, pbmsg proto.Message) (fn stateFn, masterLost bool) {
	if master, changed := d.tryUpdateMaster(ctx, pbmsg); changed {
		if master != nil {
			fn = d.postDetectionStateFn()
		} else {
			masterLost = true
			fn = RUNNING.initialState()
		}
	}
	return
}

// update the driver master info with the new value, only if it's different, and return the new value.
// otherwise return false.
func (s *schedulerDriver) tryUpdateMaster(ctx context.Context, pbmsg proto.Message) (*mesos.MasterInfo, bool) {
	if !reflect.DeepEqual(s.master, pbmsg) {
		if pbmsg == nil {
			// lost our leading master
			s.master = nil
		} else {
			// new master in town..
			s.master = pbmsg.(*mesos.MasterInfo)
		}
		return s.master, true
	}
	// else noop, master didn't really change
	return s.master, false
}

func (s *schedulerDriver) handleAuthCompleted(ctx context.Context, pbmsg proto.Message) (fn stateFn) {
	panic("not yet implemented")
}

func (s *schedulerDriver) handleRegistered(ctx context.Context, pbmsg proto.Message) stateFn {
	msg := pbmsg.(*mesos.FrameworkRegisteredMessage)
	s.framework.Id = msg.GetFrameworkId() // generated by master.
	s.sched.Registered(s.stub, s.framework.Id, msg.GetMasterInfo())
	return RUNNING.connectedStateFn
}

func (s *schedulerDriver) handleReregistered(ctx context.Context, pbmsg proto.Message) stateFn {
	msg := pbmsg.(*mesos.FrameworkRegisteredMessage)
	//TODO(jdef) assert frameworkId from masterInfo matches what we have on record?
	s.sched.Reregistered(s.stub, msg.GetMasterInfo())
	return RUNNING.connectedStateFn
}
