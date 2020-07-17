package main

import (
	"errors"
	"fmt"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Produce a test message to given broker and topic
func doProduce(broker Broker, topic string) (message string, err error) {

	// --
	// Create Producer instance
	// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#NewProducer

	// Store the config
	c := kafka.ConfigMap{
		"bootstrap.servers": broker.String()}

	// Check for errors in creating the Producer
	if p, e := kafka.NewProducer(&c); e != nil {
		if ke, ok := e.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			case kafka.ErrInvalidArg:
				return "", fmt.Errorf("😢 Can't create the producer because you've configured it wrong (code: %d)!\n\t%v\n\nTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md", ec, e)
			default:
				return "", fmt.Errorf("😢 Can't create the producer (Kafka error code %d)\n\tError: %v", ec, e)
			}
		} else {
			// It's not a kafka.Error
			return "", fmt.Errorf("😢 Oh noes, there's a generic error creating the Producer! %v", e.Error())
		}

	} else {

		defer p.Close()

		// For signalling termination from main to go-routine
		termChan := make(chan bool, 1)
		// For signalling that termination is done from go-routine to main
		doneChan := make(chan bool)
		// For capturing errors from the go-routine
		errorChan := make(chan string, 8)

		// --
		// Send a message using Produce()
		// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#Producer.Produce
		//
		// Build the message
		m := kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic},
			Value: []byte(fmt.Sprintf("foo / %v",
				time.Now().Format(time.RFC1123Z)))}
		var msg string
		// Handle any events that we get
		go func() {
			doTerm := false
			for !doTerm {
				// The `select` blocks until one of the `case` conditions
				// are met - therefore we run it in a Go Routine.
				select {
				case ev := <-p.Events():
					// Look at the type of Event we've received
					switch ev.(type) {

					case *kafka.Message:
						// It's a delivery report
						km := ev.(*kafka.Message)
						if km.TopicPartition.Error != nil {
							errorChan <- fmt.Sprintf("\n**☠️ Failed to send message '%v' to topic '%v'\n\tErr: %v",
								string(km.Value),
								string(*km.TopicPartition.Topic),
								km.TopicPartition.Error)

						} else {
							fmt.Printf("✔️ Message '%v' delivered to topic '%v' (partition %d at offset %d)\n",
								string(km.Value),
								string(*km.TopicPartition.Topic),
								km.TopicPartition.Partition,
								km.TopicPartition.Offset)
							msg = string(km.Value)
						}

					case kafka.Error:
						// It's an error
						em := ev.(kafka.Error)
						errorChan <- fmt.Sprintf("\n☠️ Uh oh, caught an error:\n\t%v", em)

					default:
						// It's not anything we were expecting
						errorChan <- fmt.Sprintf("\nGot an event that's not a Message or Error 👻\n\t%v", ev)

					}
				case <-termChan:
					doTerm = true

				}
			}
			close(errorChan)
			close(doneChan)
		}()

		// Produce the message
		if e := p.Produce(&m, nil); e != nil {
			errorChan <- fmt.Sprintf("😢 Darn, there's an error producing the message! %v", e.Error())
		}

		// --
		// Flush the Producer queue
		t := 5000
		if r := p.Flush(t); r > 0 {
			errorChan <- fmt.Sprintf("⚠️ Failed to flush all messages after %d milliseconds. %d message(s) remain", t, r)

		} else {
			fmt.Println("✨ All messages flushed from the queue")
		}
		// --
		// Stop listening to events and close the producer
		// We're ready to finish
		termChan <- true
		// wait for go-routine to terminate
		<-doneChan
		// Now we can get ready to exit
		//
		// Wrapping up…

		// When we're ready to return, check if the go routine has sent errors
		// Note that we're relying on the Go routine to close the channel, otherwise
		// we deadlock.
		// If there are no errors then the channel is simply closed and we read no values.
		done := false
		var e string
		for !done {
			if t, o := <-errorChan; o == false {
				// o is false if we've read all the values and the channel is closed
				// If that's the case, then we're done here
				done = true
			} else {
				// We've read a value so let's concatenate it with the others
				// that we've got
				e += ("\n" + t)
			}
		}

		if len(e) > 0 {
			// If we've got any errors, then return an error to the caller

			fmt.Printf("❌ Returning an error from the Producer\n")
			return "", errors.New(e)
		}

		// assuming everything has gone ok return no error
		return msg, nil

	}

}
