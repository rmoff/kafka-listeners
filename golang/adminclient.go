package main

import (
	"context"
	"fmt"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func doAdmin(broker Broker) error {

	// --
	// Create AdminClient instance
	// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#NewAdminClient

	// Store the config
	cm := kafka.ConfigMap{
		"bootstrap.servers": broker.String()}

	// Variable p holds the new AdminClient instance.
	a, e := kafka.NewAdminClient(&cm)
	// Make sure we close it when we're done
	defer a.Close()

	// Check for errors in creating the AdminClient
	if e != nil {
		if ke, ok := e.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			case kafka.ErrInvalidArg:
				return fmt.Errorf("😢 Can't create the AdminClient because you've configured it wrong (code: %d)!\n\t%v\n\nTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md", ec, e)
			default:
				return fmt.Errorf("😢 Can't create the AdminClient (Kafka error code %d)\n\tError: %v", ec, e)
			}
		} else {
			// It's not a kafka.Error
			return fmt.Errorf("😢 Oh noes, there's a generic error creating the AdminClient! %v", e.Error())
		}

	} else {

		fmt.Println("✔️ Created AdminClient")

		// Get some metadata
		if md, e := a.GetMetadata(nil, false, 5000); e != nil {
			if ke, ok := e.(kafka.Error); ok == true {
				switch ec := ke.Code(); ec {
				case kafka.ErrTransport:
					return fmt.Errorf("😢 Error (%v) getting cluster Metadata.\nIs %v a valid broker and reachable from the machine on which this is running?", e, broker)
				default:
					return fmt.Errorf("😢 Error getting cluster Metadata\n\tError: %v", e)
				}
			} else {
				// It's not a kafka.Error
				return fmt.Errorf("😢 Error getting cluster Metadata\n\tError: %v", e)
			}

		} else {
			// Print the originating broker info
			fmt.Printf("✔️ Metadata - Originating broker [i.e. the broker to which you're connected here]\n")
			b := md.OriginatingBroker
			fmt.Printf("\t[ID %d] %v\n", b.ID, b.Host)

			// Print the brokers
			fmt.Printf("✔️ Metadata - Brokers [i.e. the advertised listeners of the brokers in the cluster]\n")
			f := false
			for _, b := range md.Brokers {
				fmt.Printf("\t[ID %d] %v:%d\n", b.ID, b.Host, b.Port)
				if (b.Host == broker.host) && (b.Port == broker.port) {
					f = true
				}

			}
			if f == false {
				fmt.Printf("\n😱 😱 😱 😱 😱 😱 😱 😱 \n🛑 None of the advertised listeners on the cluster match the broker (%v) to which you're connecting.\n\nYou're gonna have a bad time trying to produce or consumer with the config like this.\n\n🔗 Check out https://www.confluent.io/blog/kafka-client-cannot-connect-to-broker-on-aws-on-docker-etc to understand more", broker)
			}
			// // Print the topics
			// fmt.Printf("✔️ Metadata - topics\n")
			// for _, t := range md.Topics {
			// 	fmt.Printf("\t(%v partitions)\t%v\n", len(t.Partitions), t.Topic)
			// }
		}

		// Create a context for use when calling some of these functions
		// This lets you set a variable timeout on invoking these calls
		// If the timeout passes then an error is returned.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get the ClusterID
		if c, e := a.ClusterID(ctx); e != nil {
			return fmt.Errorf("😢 Error getting ClusterID\n\tError: %v", e)
		} else {
			fmt.Printf("\n✔️ ClusterID: %v\n", c)
		}

		// Start the context timer again (otherwise it carries on from the original deadline)
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get the ControllerID
		if c, e := a.ControllerID(ctx); e != nil {
			return fmt.Errorf("😢 Error getting ControllerID\n\tError: %v", e)
		} else {
			fmt.Printf("✔️ ControllerID: %v\n", c)
		}

		// 👋 … and we're done
		return nil
	}
}
