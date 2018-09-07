package queue

import (
	"log"

	"github.com/astaxie/beego"
	"github.com/streadway/amqp"
)

var con *amqp.Connection

func init() {
	if err := setConnection(beego.AppConfig.String("rabbitDB")); err != nil {
		log.Panicln(err)
	}
}

func setConnection(url string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	con = conn
	return nil
}

func GetConnection() *amqp.Connection {
	return con
}
