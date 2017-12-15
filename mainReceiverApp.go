package main

import (
	"github.com/openenergi/go-event-hub/eventhub"
	"log"
	"os"
	"time"
)

const (
	ehNamespace         = "<FILL_THIS_WITH_THE_RELEVANT_DETAILS>"
	ehName              = "<FILL_THIS_WITH_THE_RELEVANT_DETAILS>"
	superUserPolicy     = "<FILL_THIS_WITH_THE_RELEVANT_DETAILS>"
	superUserPrimaryKey = "<FILL_THIS_WITH_THE_RELEVANT_DETAILS>"
	consumerGroupName   = "<FILL_THIS_WITH_THE_RELEVANT_DETAILS>"
	// make sure the following CSV file contains the same
	// number of Partitions specified on your Azure portal
	offsetsRelPath       = "./assets/main_receiver_offsets.csv"
	offsetsFlushInterval = 1 * time.Second
)

func main() {
	eventhub.Logger = log.New(os.Stdout, "[Event Hub] ", log.LstdFlags)
	appLogger := log.New(os.Stdout, "[Eh Receiver] ", log.LstdFlags)

	ehReceiver, err := eventhub.NewReceiver(eventhub.ReceiverOpts{
		EventHubNamespace: ehNamespace,
		EventHubName:      ehName,
		SasPolicyName:     superUserPolicy,
		SasPolicyKey:      superUserPrimaryKey,
		ConsumerGroupName: consumerGroupName,
		// PartitionOffsets:     []string{"",""},//{"2440","1496"},
		PartitionOffsetsPath: "./assets/main_receiver_offsets.csv",
		OffsetsFlushInterval: 800 * time.Millisecond,
		TokenExpiryInterval:  20 * time.Second,
		Debug:                true,
	})
	if err != nil {
		panic(err)
	}
	defer ehReceiver.Close()

	go func(r eventhub.Receiver) {
		appLogger.Printf("Setting up the error channel...\n")
		for err := range r.ErrorChan() {
			if err != nil {
				appLogger.Printf("Just received an error: '%v'\n", err)
				panic(err)
			}
		}
	}(ehReceiver)

	appLogger.Printf("Starting the consumer of messages...\n")
	ehReceiver.AsyncFetch()
	for currEhMsg := range ehReceiver.ReceiveChan() {
		appLogger.Printf("Got a message: '%v'\n", currEhMsg)
	}
}
