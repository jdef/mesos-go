package sched

import (
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

// implements SchedulerDriver interface, the API that framework schedulers use
// to communicate with mesos. all API calls are executed synchronously, waiting
// for a response from the driver binding.
type schedulerDriverStub struct {
	ops           chan<- opRequest
	status        statusType // cached from most recent driver call
	frameworkInfo mesos.FrameworkInfo
	cache         *schedCache // concurrent cache
	done          <-chan struct{}
}

func (s *schedulerDriverStub) execAndWait(req opRequest) (mesos.Status, error) {
	ch := make(chan opResponse, 1)
	req.out = ch
	select {
	case <-s.done:
		return mesos.Status(ABORTED), terminatedError
	case s.ops <- req:
		select {
		case <-s.done:
			return mesos.Status(ABORTED), terminatedError
		case resp, ok := <-ch:
			if !ok {
				panic("response channel closed")
			} else {
				defer atomic.StoreInt32((*int32)(&s.status), int32(resp.status))
				return mesos.Status(resp.status), resp.err
			}
		}
	}
}

func (s *schedulerDriverStub) _status() mesos.Status {
	return mesos.Status(atomic.LoadInt32((*int32)(&s.status)))
}

//
// SchedulerDriver implementation
//
//

func (s *schedulerDriverStub) Start() (mesos.Status, error) {
	return s.execAndWait(opRequest{opcode: startOp})
}

func (s *schedulerDriverStub) Stop(failover bool) (mesos.Status, error) {
	panic("not yet implemented")
}

func (s *schedulerDriverStub) Join() (mesos.Status, error) {
	panic("not yet implemented")
}

func (s *schedulerDriverStub) Run() (status mesos.Status, err error) {
	if status, err = s.Start(); err == nil {
		status, err = s.Join()
	}
	return
}

func (s *schedulerDriverStub) Abort() (mesos.Status, error) {
	panic("not yet implemented")
}

func (s *schedulerDriverStub) SendFrameworkMessage(executorID *mesos.ExecutorID, slaveID *mesos.SlaveID, data string) (mesos.Status, error) {
	// TODO(jdef) build ExecutorToFrameworkMessage
	var msg *mesos.ExecutorToFrameworkMessage
	return s.execAndWait(opRequest{opcode: frameworkMsgOp, msg: msg})
}

func (s *schedulerDriverStub) LaunchTasks(offers []*mesos.OfferID, tasks []*mesos.TaskInfo, filters *mesos.Filters) (mesos.Status, error) {
	if msg, err := s.buildLaunchTasks(offers, tasks, filters); err != nil {
		return s._status(), err
	} else {
		return s.execAndWait(opRequest{opcode: launchTasksOp, msg: msg})
	}
}

func (s *schedulerDriverStub) RequestResources(requests []*mesos.Request) (mesos.Status, error) {
	panic("not yet implemented")
}

func (s *schedulerDriverStub) KillTask(taskID *mesos.TaskID) (mesos.Status, error) {
	panic("not yet implemented")
}

func (s *schedulerDriverStub) DeclineOffer(offerID *mesos.OfferID, filters *mesos.Filters) (mesos.Status, error) {
	panic("not yet implemented")
}

func (s *schedulerDriverStub) ReviveOffers() (mesos.Status, error) { panic("not yet implemented") }

func (s *schedulerDriverStub) ReconcileTasks(statuses []*mesos.TaskStatus) (mesos.Status, error) {
	panic("not yet implemented")
}

//
// message builders
//
//

func (s *schedulerDriverStub) buildLaunchTasks(offers []*mesos.OfferID, tasks []*mesos.TaskInfo, filters *mesos.Filters) (proto.Message, error) {
	okTasks := make([]*mesos.TaskInfo, 0, len(tasks))

	// Set TaskInfo.executor.framework_id, if it's missing.
	for _, task := range tasks {
		if task.Executor != nil && task.Executor.FrameworkId == nil {
			// avoid modifying the original task, make a copy and update that
			task = &mesos.TaskInfo{
				Name:      task.Name,
				TaskId:    task.TaskId,
				SlaveId:   task.SlaveId,
				Resources: task.Resources,
				Executor: &mesos.ExecutorInfo{
					ExecutorId:  task.Executor.ExecutorId,
					FrameworkId: s.frameworkInfo.Id,
					Command:     task.Executor.Command,
					Container:   task.Executor.Container,
					Resources:   task.Executor.Resources,
					Name:        task.Executor.Name,
					Source:      task.Executor.Source,
					Data:        task.Executor.Data,
				},
				Command:     task.Command,
				Container:   task.Container,
				Data:        task.Data,
				HealthCheck: task.HealthCheck,
			}
		}
		okTasks = append(okTasks, task)
	}
	for _, offerId := range offers {
		for _, task := range okTasks {
			// Keep only the slave PIDs where we run tasks so we can send
			// framework messages directly.
			if cached, found := s.cache.getOffer(offerId); found {
				if cached.offer.SlaveId.Equal(task.SlaveId) {
					// cache the tasked slave, for future communication
					pid := cached.slavePid
					s.cache.putSlavePid(task.SlaveId, pid)
				} else {
					log.Warningf("Attempting to launch task %s with the wrong slaveId offer %s\n", task.TaskId.GetValue(), task.SlaveId.GetValue())
				}
			} else {
				log.Warningf("Attempting to launch task %s with unknown offer %s\n", task.TaskId.GetValue(), offerId.GetValue())
			}
		}
		s.cache.removeOffer(offerId) // if offer
	}
	message := &mesos.LaunchTasksMessage{
		FrameworkId: s.frameworkInfo.Id,
		OfferIds:    offers,
		Tasks:       okTasks,
		Filters:     filters,
	}
	return message, nil
}
