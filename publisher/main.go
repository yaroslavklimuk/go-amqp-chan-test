package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

const EXCHANGE_NAME = "test_channels_exchange"
const AMQP_URI = "amqp://user:pass@rabbitmq:5672/"

var connection *amqp.Connection
var channel *amqp.Channel

func main() {
	conn, err := getConnection(AMQP_URI)
	if err != nil {
		panic(err)
	}
	ch, err := getChannel(conn)
	if err != nil {
		panic(err)
	}
	for {
		err = ch.Publish(
			EXCHANGE_NAME,
			"",
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte("shiny happy channel is holding a connection"),
			},
		)
		if err != nil {
			fmt.Printf("error: %s\n", err.Error())
		}
		time.Sleep(time.Second * 20)
	}
}

func getConnection(uri string) (*amqp.Connection, error) {
	var err error
	if connection == nil {
		connection, err = amqp.Dial(uri)
	}

	return connection, err
}

func getChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	var err error
	if channel == nil {
		channel, err = conn.Channel()
	}
	return channel, err
}
