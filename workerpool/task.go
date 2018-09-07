package workerpool

import (
	"sync"
)

type Task struct {
	// Err holds an error that occurred during a task. Its
	// result is only meaningful after Run has been called
	// for the pool that holds it.
	Err error

	f func() error
}

// NewTask initializes a new task based on a given work
// function.
func NewTask(f func() error) *Task {
	return &Task{f: f}
}

// Run runs a Task and does appropriate accounting via a
// given sync.WorkGroup.
func (t *Task) Run(wg *sync.WaitGroup) {
	t.Err = t.f()
	wg.Done()
}
