package main

import (
	"fmt"
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
)

func main() {
	eventhub.Logger = log.New(os.Stdout, "[Event Hub] ", log.LstdFlags)
	appLogger := log.New(os.Stdout, "[Eh Sender] ", log.LstdFlags)

	ehSender, err := eventhub.NewSender(eventhub.SenderOpts{
		EventHubNamespace:   ehNamespace,
		EventHubName:        ehName,
		SasPolicyName:       superUserPolicy,
		SasPolicyKey:        superUserPrimaryKey,
		TokenExpiryInterval: 20 * time.Second,
		Debug:               true,
	})
	if err != nil {
		panic(err)
	}
	defer ehSender.Close()

	go func(s eventhub.Sender) {
		appLogger.Printf("Setting up the error channel...\n")
		for err := range s.ErrorChan() {
			if err != nil {
				appLogger.Printf("Just received an error: '%v'\n", err)
				panic(err)
			}
		}
	}(ehSender)

	appLogger.Printf("Now sending a message!\n")
	thisMessage := fmt.Sprintf("Sending just ONE at timestamp %v", time.Now())

	// TODO change the sleep with a "quit channel"
	time.Sleep(2 * time.Second)

	// 1) send sync
	// ------------
	uniqueId, err := ehSender.Send(thisMessage)
	if err != nil {
		appLogger.Printf("!!! error sending '%v', the error message is: %v\n", thisMessage, err)
	}
	appLogger.Printf("The message was identified by Send with this ID: %d\n", uniqueId)

	// TODO change the sleep with a "quit channel"
	time.Sleep(2 * time.Second)

	// 2) send async (wait forever for ACK)
	// ------------------------------------
	uniqueId = ehSender.SendAsync(thisMessage)
	appLogger.Printf("The message was identified by SendAsync with this ID: %d\n", uniqueId)

	// 3) send async with timeout
	// --------------------------
	uniqueId = ehSender.SendAsyncTimeout(thisMessage, 2*time.Second)
	appLogger.Printf("The message was identified by SendAsyncTimeout with this ID: %d\n", uniqueId)

	select {}
}
