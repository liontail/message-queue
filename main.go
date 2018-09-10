package main

import (
	"message-queue/consumer"
	"message-queue/workerpool"
)

func main() {

	tasks := []*workerpool.Task{
		workerpool.NewTask(func() {
			c := consumer.Consumer{ID: 1}
			c.Consume()

		}),
		workerpool.NewTask(func() {
			c := consumer.Consumer{ID: 2}
			c.Consume()

		}),
		workerpool.NewTask(func() {
			c := consumer.Consumer{ID: 3}
			c.Consume()

		}),
	}

	p := workerpool.NewPool(tasks, 3)
	p.Run()

	// var numErrors int
	// for _, task := range p.Tasks {
	// 	if task.Err != nil {
	// 		beego.Error(task.Err)
	// 		numErrors++
	// 	}
	// 	if numErrors >= 10 {
	// 		beego.Error("Too many errors.")
	// 		break
	// 	}
	// }
}
