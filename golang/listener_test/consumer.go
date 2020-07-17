package main

import (
	"fmt"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func doConsume(broker Broker, topic, message string) error {

	fmt.Printf("Starting consumer, 👀 looking for specific message:\n\t%v\n\n", message)
	// --
	// Create Consumer instance
	// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#NewConsumer

	// Store the config
	cm := kafka.ConfigMap{
		"bootstrap.servers":    broker.String(),
		"group.id":             "rmoff_learning_go_foo",
		"enable.partition.eof": true}

	// Variable p holds the new Consumer instance.
	c, e := kafka.NewConsumer(&cm)

	// Check for errors in creating the Consumer
	if e != nil {
		if ke, ok := e.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			case kafka.ErrInvalidArg:
				return fmt.Errorf("😢 Can't create the Consumer because you've configured it wrong (code: %d)!\n\t%v\n\nTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md", ec, e)
			default:
				return fmt.Errorf("😢 Can't create the Consumer (Kafka error code %d)\n\tError: %v", ec, e)
			}
		} else {
			// It's not a kafka.Error
			return fmt.Errorf("😢 Oh noes, there's a generic error creating the Consumer! %v", e.Error())
		}

	} else {
		// make sure that we close the Consumer when we exit
		defer c.Close()

		// Subscribe to the topic
		if e := c.Subscribe(topic, nil); e != nil {
			return fmt.Errorf("☠️ Uh oh, there was an error subscribing to the topic :\n\t%v", e)
		}
		fmt.Printf("Subscribed to topic %v", topic)

		doTerm := false
		for !doTerm {
			ev := c.Poll(1000)
			if ev == nil {
				// the Poll timed out and we got nothin'
				fmt.Printf("……\n")
				a, _ := c.Assignment()
				p, _ := c.Position(a)

				for _, x := range p {
					fmt.Printf("Partition %v position %v\n", x.Partition, x.Offset)
				}

				continue
			} else {
				// The poll pulled an event, let's now
				// look at the type of Event we've received
				switch ev.(type) {

				case *kafka.Message:
					// It's a message
					km := ev.(*kafka.Message)
					fmt.Printf("✅ Message '%v' received from topic '%v' (partition %d at offset %d)\n",
						string(km.Value),
						string(*km.TopicPartition.Topic),
						km.TopicPartition.Partition,
						km.TopicPartition.Offset)
					if message == string(km.Value) {
						fmt.Println("✔️ Read the message we were waiting for. Closing the consumer.")
						doTerm = true
					}

				case kafka.PartitionEOF:
					pe := ev.(kafka.PartitionEOF)
					fmt.Printf("🌆 Got to the end of partition %v on topic %v at offset %v\n",
						pe.Partition,
						string(*pe.Topic),
						pe.Offset)

				case kafka.OffsetsCommitted:
					continue

				case kafka.Error:
					// It's an error
					em := ev.(kafka.Error)
					return fmt.Errorf("☠️ Uh oh, caught an error:\n\t%v", em)

				default:
					// It's not anything we were expecting
					return fmt.Errorf("Got an event that's not a Message, Error, or PartitionEOF 👻\n\t%v", ev)

				}

			}
		}
		// 👋 … and we're done
		return nil

	}

}
