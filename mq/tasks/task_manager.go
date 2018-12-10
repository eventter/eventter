package tasks

import (
	"context"
	"log"
	"sync/atomic"
)

var currentTaskID uint64

type TaskManager struct {
	name          string
	managerCtx    context.Context
	cancelAll     func()
	currentID     uint64
	Completed     <-chan *Task
	sendCompleted chan<- *Task
	closed        uint32
}

func NewManager(ctx context.Context, name string) *TaskManager {
	managerCtx, cancelAll := context.WithCancel(ctx)
	completed := make(chan *Task, 128)
	return &TaskManager{
		name:          name,
		managerCtx:    managerCtx,
		cancelAll:     cancelAll,
		Completed:     completed,
		sendCompleted: completed,
	}
}

func (m *TaskManager) Start(name string, run func(ctx context.Context) error, data interface{}) *Task {
	ctx, cancel := context.WithCancel(m.managerCtx)
	task := &Task{
		ID:     atomic.AddUint64(&currentTaskID, 1),
		Name:   name,
		Data:   data,
		Cancel: cancel,
	}

	go func() {
		defer func() {
			cancel()
			if atomic.LoadUint32(&m.closed) == 0 {
				m.sendCompleted <- task
			}
		}()
		err := run(ctx)
		if err == nil {
			log.Printf("[%s] task %d (%s) completed successfully", m.name, task.ID, task.Name)
		} else {
			task.Err = err
			log.Printf("[%s] task %d (%s) failed: %v", m.name, task.ID, task.Name, err)
		}
	}()

	log.Printf("[%s] task %d (%s) started", m.name, task.ID, task.Name)

	return task
}

func (m *TaskManager) Close() error {
	atomic.StoreUint32(&m.closed, 1)
	m.cancelAll()
	return nil
}
