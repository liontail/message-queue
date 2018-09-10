package workerpool

import (
	"sync"
)

type Task struct {
	f func()
}

// NewTask initializes a new task based on a given work
// function.
func NewTask(f func()) *Task {
	return &Task{f: f}
}

// Run runs a Task and does appropriate accounting via a
// given sync.WorkGroup.
func (t *Task) Run(wg *sync.WaitGroup) {
	t.f()
	wg.Done()
}
