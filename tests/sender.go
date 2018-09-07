package main

import (
	"log"
	"message-queue/queue"
	"message-queue/workerpool"

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
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	for i := 0; i < 1000; i++ {

		body := `{
			"id": "239_tw_965959199321505792_1037384853317025793",
			"mid": "tw_965959199321505792_1037384853317025793",
			"channel": "twitter",
			"platform": "twitter",
			"post_type": "text",
			"data_type": "twitter_status",
			"type": 5,
			"is_topic": false,
			"topic": "Retweeted",
			"desc": "RT @KewaKewa2: นั่งดูคอนใน <em>AIS</em> <em>PLAY</em> เจอผช.คนนี้หน้าคุ้นๆเนาะ หน้าเหมือนผัวเราเลย 😋\n#เป๊กผลิตโชค \n#peckpalitchoke https://t.co/4zv9mDMrrA",
			"langs": [
			    {
				"code": "th",
				"percent": 76
			    },
			    {
				"code": "en",
				"percent": 18
			    },
			    {
				"code": "ha",
				"percent": 4
			    }
			],
			"link": "http://twitter.com/annrin13/status/1037384853317025793",
			"alphabet_langs": [
			    {
				"code": "eng",
				"percent": 37
			    },
			    {
				"code": "tha",
				"percent": 50
			    }
			],
			"domain": "twitter.com",
			"info": {
			    "default_country": "th",
			    "from": {
				"_id": "twitter_965959199321505792",
				"gender": 0
			    }
			},
			"z": {
			    "engage": 0
			},
			"read": false,
			"hide": false,
			"created_time": 1536166797000,
			"sys_time": 1536172066115,
			"cts": 1536171291313,
			"cursor": "5b900b8ddaf8594f188c73a3",
			"account_id": "239",
			"keywords": [
			    {
				"id": "5aa4a29df04f4908719935dd",
				"name": "AIS play",
				"sentiment": 0
			    },
			    {
				"id": "5aa4a29df04f4908719935de",
				"name": "AIS HBO",
				"sentiment": 0
			    },
			    {
				"id": "5aa4a29df04f4908719935db",
				"name": "AIS",
				"sentiment": 0
			    },
			    {
				"id": "5aa4a29bf04f4908719935bc",
				"name": "apple",
				"sentiment": 0,
				"tags": [
				    {
					"_id": "5b20deda654e95035bbd3ca2",
					"name": "storage 64 gb"
				    },
				    {
					"_id": "5b20dec2654e95035bbd3c9d",
					"name": "phone spec"
				    },
				    {
					"_id": "5b20df28654e95035bbd3cac",
					"name": "storage 128 gb"
				    }
				]
			    }
			],
			"from_dump": false,
			"insert_es_time": "0001-01-01T00:00:00Z",
			"social_id": "965959199321505792"
		    }`
		// for j := 0; j < 5; j++ {
		// 	body += body
		// }
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
		workerpool.NewTask(func() error {
			sender(1)
			return nil
		}),
		workerpool.NewTask(func() error {
			sender(2)
			return nil
		}),
		workerpool.NewTask(func() error {
			sender(3)
			return nil
		}),
	}

	p := workerpool.NewPool(tasks, 3)
	p.Run()

	var numErrors int
	for _, task := range p.Tasks {
		if task.Err != nil {
			beego.Error(task.Err)
			numErrors++
		}
		if numErrors >= 10 {
			beego.Error("Too many errors.")
			break
		}
	}
}
