package main

import (
	"message-queue/consumer"
	"message-queue/workerpool"

	"github.com/astaxie/beego"
)

func main() {
	workers := []*workerpool.Task{}
	maxWorkers := beego.AppConfig.DefaultInt("MAX_WORKER", 1)
	for i := 0; i < maxWorkers; i++ {
		id := i + 1
		workers = append(workers, workerpool.NewTask(func() {
			c := consumer.Consumer{ID: id}
			c.Consume()
		}))
	}
	p := workerpool.NewPool(workers, maxWorkers)
	p.Run()
}
