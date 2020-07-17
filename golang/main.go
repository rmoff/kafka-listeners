// Utility that checks Kafka broker advertised listener config
//
// See https://rmoff.net/2020/07/17/learning-golang-some-rough-notes-s02e08-checking-kafka-advertised.listeners-with-golang/
// and https://www.confluent.io/blog/kafka-client-cannot-connect-to-broker-on-aws-on-docker-etc
//
// @rmoff 17 July 2020
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Broker struct {
	host string
	port int
}

func (b Broker) String() string {
	return fmt.Sprintf("%v:%v", b.host, b.port)
}

func main() {

	// Set the broker based on defaults or commandline arg if provided
	broker := setBroker(os.Args)
	fmt.Printf("Using Broker: %v\n--------------------------\n\n", broker)

	// Set the topic name that we'll use
	topic := "rmoff_test_00"

	// Create Admin Connection
	if e := doAdmin(broker); e != nil {
		fmt.Printf("❌There was a problem calling the Admin Client:\n%v\n", e)
	} else {
		// Produce message
		fmt.Printf("✅ AdminClient worked\n--------------------------\n\n")
		if m, e := doProduce(broker, topic); e != nil {
			fmt.Printf("❌There was a problem calling the producer:\n%v\n", e)
		} else {
			fmt.Printf("✅ Producer worked\n--------------------------\n\n")
			// Consume message
			if e := doConsume(broker, topic, m); e != nil {
				fmt.Printf("❌There was a problem calling the consumer:\n%v\n", e)
			} else {
				fmt.Printf("✅ Consumer worked\n--------------------------\n\n")

				// Consume message

			}
		}

	}
	// fin.

}

func setBroker(i []string) Broker {

	b := Broker{
		host: "localhost",
		port: 9092}

	// Try to get the broker details from the commandline arg
	// The validation and error checking here is rudimentary.
	if len(i) == 2 {
		// hard coded to expect a single user-provided arg (the first arg is the program name)
		a := i[1]
		// Check that there's a colon (i.e. we're hopefully going to get host:port)
		if p := strings.Split(a, ":"); len(p) == 2 {
			// Check that the port is an integer
			if _, err := strconv.Atoi(p[1]); err == nil {
				b.port, _ = strconv.Atoi(p[1])
				b.host = p[0]
			} else {
				fmt.Printf("(%v is not an integer, so ignoring provided value and defaulting to %v)\n", p[1], b)
			}
		} else {
			fmt.Printf("(Commandline value %v doesn't look like a host:port, so defaulting to %v)\n", a, b)
		}
	} else {
		fmt.Printf("(A single commandline argument should be used to specify the broker. Defaulting to %v)\n", b)
	}
	return b

}
