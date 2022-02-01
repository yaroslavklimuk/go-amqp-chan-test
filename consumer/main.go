package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"strconv"
)

const QUEUE_NAME = "test_channels_queue"
const CONSUMER_NAME = "test_chan_consumer"
const AMQP_URI = "amqp://user:pass@rabbitmq:5672/"

var connection *amqp.Connection
var channel *amqp.Channel

func main() {
	consumerThreads := 2
	deliveries := make([]<-chan amqp.Delivery, consumerThreads)
	for i := 0; i < consumerThreads; i++ {
		consumer := CONSUMER_NAME + "_" + strconv.Itoa(i)
		delivery, err := consume(AMQP_URI, consumer)
		if err != nil {
			panic(err)
		}
		deliveries[i] = delivery
		go func(amqpCh <-chan amqp.Delivery) {
			for item := range amqpCh {
				fmt.Printf("cons: %s; msg - %s\n", consumer, string(item.Body))
				err := item.Ack(true)
				if err != nil {
					fmt.Println("err: failed to ack a message")
				}
			}
		}(deliveries[i])
	}
	defer connection.Close()
	select {}
}

func consume(uri string, consumer string) (<-chan amqp.Delivery, error) {
	conn, err := getConnection(uri)
	if err != nil {
		return nil, err
	}
	ch, err := getChannel(conn)
	if err != nil {
		return nil, err
	}
	return ch.Consume(
		QUEUE_NAME,
		consumer,
		false,
		false,
		false,
		false,
		nil,
	)
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
