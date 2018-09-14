package main

import (
	"log"
	"message-queue/queue"
	"message-queue/workerpool"
	"strconv"

	"github.com/astaxie/beego"
	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func sender(senderID uint) {

	conn := queue.GetConnection()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		beego.AppConfig.String("queueName"), // name
		false,                               // durable
		false,                               // delete when unused
		false,                               // exclusive
		false,                               // no-wait
		nil,                                 // arguments
	)
	failOnError(err, "Failed to declare a queue")

	for i := 0; i < 1000; i++ {

		body := strconv.Itoa(i)
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
		log.Printf("[%d] Sent %d", senderID, i)
		failOnError(err, "Failed to publish a message")
	}
}

func main() {
	tasks := []*workerpool.Task{
		workerpool.NewTask(func() {
			sender(1)
		}),
	}

	p := workerpool.NewPool(tasks, len(tasks))
	p.Run()

}
