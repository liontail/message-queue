package consumer

import (
	"log"
	"message-queue/queue"
)

type Consumer struct {
	ID uint
}

func (consumer *Consumer) failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func (consumer *Consumer) Consume() {
	// conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	// consumer.failOnError(err, "Failed to connect to RabbitMQ")
	// defer conn.Close()
	conn := queue.GetConnection()

	ch, err := conn.Channel()
	consumer.failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	consumer.failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	consumer.failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Consumer ID: %d: Received a message: ==>%s", consumer.ID, d.Body)
		}
	}()

	log.Printf("Worker ID: %d: is ready", consumer.ID)
	<-forever
}
