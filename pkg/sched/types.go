package sched

import (
	"errors"
	"fmt"

	"github.com/gogo/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/messenger"
	"github.com/mesos/mesos-go/upid"
	"golang.org/x/net/context"
)

type opType int

// these align with the states in mesosProto
const (
	startOp opType = iota
	stopOp
	abortOp
	joinOp
	runOp
	requestResourcesOp
	launchTasksOp
	killTaskOp
	acceptOffersOp
	declineOfferOp
	reviveOffersOp
	frameworkMsgOp
	reconcileTasksOp
	__internalOps // all ops after this don't generate response, TODO(jdef) why aren't these callbacks?
	handleCallbackOp
	masterDetectedOp
	authCompletedOp
)

type callbackType int

const (
	disconnectedCb callbackType = iota
	registeredCb
	reregisteredCb
	offerRescindedCb
	resourceOffersCb
	statusUpdateCb
	frameworkMessageCb
	slaveLostCb
	executorLostCb
	frameworkErrorCb
)

type ReasonCode int

const (
	NotRunning ReasonCode = iota
	NotConnected
)

type IllegalStateError struct {
	Status mesos.Status
	Reason ReasonCode
}

var (
	terminatedError        = errors.New("driver has terminated")
	frameworkRequiredError = errors.New("mesos FrameworkInfo is required and may not be nil")
)

// convenience
type statusType mesos.Status // or something like this

// more convenience
const (
	NOT_STARTED = statusType(mesos.Status_DRIVER_NOT_STARTED)
	RUNNING     = statusType(mesos.Status_DRIVER_RUNNING)
	ABORTED     = statusType(mesos.Status_DRIVER_ABORTED)
	STOPPED     = statusType(mesos.Status_DRIVER_STOPPED)

	NO_RESPONSE_REQUIRED = statusType(999)
)

// panic if the actual status doesn't match the expected status.
// should be used to guard against programming errors.
func (actual statusType) Check(expected statusType) {
	if actual != expected {
		panic(fmt.Sprintf("expected status %v instead of %v", expected, actual))
	}
}

// the driver cannot proceed with the operation given its current status
func (actual statusType) Illegal(r ReasonCode) error {
	return &IllegalStateError{
		Status: mesos.Status(actual),
		Reason: r,
	}
}

func (err *IllegalStateError) Error() string {
	//TODO(jdef) include stringified status and reason code in message
	return "the driver cannot proceed given its current status"
}

type opResponse struct {
	status statusType
	err    error
}

type opRequest struct {
	opcode opType
	out    chan<- opResponse // optional
	msg    proto.Message     // optional
	cb     *callback         // optional
}

type callback struct {
	opcode callbackType
	msg    proto.Message
	from   *upid.UPID
}

type driverConfig struct {
	framework *mesos.FrameworkInfo
	creds     *mesos.Credentials
	sched     Scheduler                           // receives callbacks from the driver
	messenger func() (messenger.Messenger, error) // factory func
}

type stateFn func(context.Context, *schedulerDriver) (stateFn, opResponse)
