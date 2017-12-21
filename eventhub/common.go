// Package eventhub allows to connect to the Azure Event Hub via AMQP 1.0,
// it then allows to send and receive messages.
package eventhub

import (
	"crypto/tls"
	"fmt"
	"github.com/openenergi/go-event-hub/msauth"
	"io/ioutil"
	"log"
	"os"
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
	"time"
)

const (
	eventHubURITemplate              = "amqp://%s.servicebus.windows.net/%s"
	eventHubDomainTemplate           = "%s.servicebus.windows.net"
	eventHubDomainPortTemplate       = "%s.servicebus.windows.net:5671"
	eventHubCbsName                  = "$cbs"
	cbsHandshakeRecurrenceLowerBound = 20 * time.Second
	sslKeyLogsRelPath                = "./sslkey.log"
)

type debugRand struct{}

func (dr *debugRand) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = byte(0)
	}
	return len(p), nil
}

func newZeroRand() *debugRand {
	return &debugRand{}
}

// tlsConfig()
func tlsConfig(debug bool) (*tls.Config, error) {
	if debug == true {
		// https://golang.org/pkg/crypto/tls/#example_Config_keyLogWriter
		w, err := os.OpenFile(sslKeyLogsRelPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return nil, err
		}
		tlsConfig := tls.Config{
			Rand:         newZeroRand(),
			KeyLogWriter: w,
		}
		Logger.Printf("The TLS configuration is storing the SSL key at '%s'\n", sslKeyLogsRelPath)
		return &tlsConfig, nil
	}
	return &tls.Config{}, nil
}

func msgProps(namespace string, name string, rawEventHubURI string) map[string]interface{} {
	// check https://github.com/Azure/amqpnetlite/blob/master/Examples/ServiceBus/Scenarios/CbsAsyncExample.cs
	appProps := make(map[string]interface{})
	appProps["operation"] = "put-token"
	appProps["type"] = "servicebus.windows.net:sastoken"
	appProps["name"] = rawEventHubURI
	return appProps
}

func newAmqpConn(container electron.Container, namespace string, debug bool) (electron.Connection, error) {
	// TLS ebabled TCP connection
	tlsConfig, err := tlsConfig(debug)
	if err != nil {
		return nil, err
	}
	ehDomainPort := fmt.Sprintf(eventHubDomainPortTemplate, namespace)
	tlsConn, err := tls.Dial("tcp", ehDomainPort, tlsConfig)
	if err != nil {
		return nil, err
	}
	// AMQP over TCP
	ehDomain := fmt.Sprintf(eventHubDomainTemplate, namespace)
	amqpConn, err := container.Connection(
		tlsConn,
		electron.VirtualHost(ehDomain),
		electron.SASLAllowedMechs("ANONYMOUS"), // this may not be needed as it's the default mechanism for the Azure Event Hub
		electron.Heartbeat(60*time.Second),
	)
	if err != nil {
		return nil, err
	}
	Logger.Printf("The AMQP connection is ready for the namespace '%v'\n", namespace)
	return amqpConn, nil
}

// ---------------------------------------------------

type handshakeOpts struct {
	Namespace            string
	Name                 string
	SasPolicyName        string
	SasPolicyKey         string
	CbsHandshakeInterval time.Duration
	AmqpConnection       electron.Connection
	ErrorChan            chan error
}

type eventHubAuth struct {
	signer               msauth.Signer
	ehURI                string
	cbsHandshakeInterval time.Duration
	currentToken         string
	msgProps             map[string]interface{}
	cbsAmqpLink          electron.Sender
	errorChan            chan error
}

// This is generating the SAS token,
// then attaching it to the instance
func (eha *eventHubAuth) renewSasToken() {
	expiryInterval := msauth.SignatureExpiry(time.Now(), eha.cbsHandshakeInterval)
	Logger.Printf("The expiry interval for the SASL token is: %s\n", expiryInterval)
	newToken := eha.signer.Sign(eha.ehURI, expiryInterval)
	Logger.Printf("The new Microsoft SASL token is: %s\n", newToken)
	eha.currentToken = newToken
}

func cbsMsg(appProps map[string]interface{}, sasToken string) amqp.Message {
	// check https://github.com/Azure/amqpnetlite/blob/master/Examples/ServiceBus/Scenarios/CbsAsyncExample.cs
	cbsHandshakeMsg := amqp.NewMessage()
	cbsHandshakeMsg.SetApplicationProperties(appProps)
	cbsHandshakeMsg.Marshal(sasToken)
	return cbsHandshakeMsg
}

func (eha *eventHubAuth) handshake() error {
	// generate and store the Microsoft token
	eha.renewSasToken()
	// prepare the AMQP message for the Microsoft CBS handshake
	cbsMsg := cbsMsg(eha.msgProps, eha.currentToken)
	// handshake via "$cbs"
	outcome := eha.cbsAmqpLink.SendSync(cbsMsg)

	return validateOutcome(outcome)
}

func validateOutcome(message electron.Outcome) error {
	if message.Error != nil {
		return message.Error
	}
	if message.Status != electron.Accepted {
		return fmt.Errorf("The CBS handshake was unsuccessful, the AMQP package got this sending status: '%s'\n", message.Status)
	}

	return nil
}

func flushAuthErrorToChan(err error, errChan chan error) {
	if err != nil {
		Logger.Printf("CBS handshake error: %v\n", err)
		errChan <- err
	}
}

func (eha *eventHubAuth) asyncScheduledHandshake() error {
	tickerHandshake := time.NewTicker(eha.cbsHandshakeInterval)
	// perform the initial handshake
	err := eha.handshake()
	if err != nil {
		Logger.Printf("Initial error on the CBS handshake. Aborting!")
		return err
	}
	go func(currEha *eventHubAuth, ticker *time.Ticker) {
		for {
			select {
			case <-ticker.C:
				// perform the recurring handshake
				err := currEha.handshake()
				flushAuthErrorToChan(err, eha.errorChan)
			}
		}
	}(eha, tickerHandshake)
	return nil
}

func newEventHubAuth(ehOpts handshakeOpts) (*eventHubAuth, error) {
	if ehOpts.CbsHandshakeInterval < cbsHandshakeRecurrenceLowerBound {
		return nil, fmt.Errorf("The CBS handshake interval must be at least: %s instead it was: %s \n", cbsHandshakeRecurrenceLowerBound, ehOpts.CbsHandshakeInterval)
	}

	// The URI follows this pattern: "amqp://<NAMESPACE>.servicebus.windows.net/<NAME>"
	ehURI := fmt.Sprintf(eventHubURITemplate, ehOpts.Namespace, ehOpts.Name)
	Logger.Printf("Using this Event Hub URI: '%s'\n", ehURI)

	// the CBS instance to return
	instance := &eventHubAuth{
		// the instance dealing with the Microsoft token
		signer:               msauth.New(ehOpts.Namespace, ehOpts.SasPolicyName, ehOpts.SasPolicyKey),
		cbsHandshakeInterval: ehOpts.CbsHandshakeInterval,
		ehURI:                ehURI,
		// the AMQP message properties shared among multiple link interactions
		msgProps:  msgProps(ehOpts.Namespace, ehOpts.Name, ehURI),
		errorChan: ehOpts.ErrorChan,
	}

	// AMQP link on the "$cbs" special EvenHub dedicated to the SASL handshake
	cbsAmqpLink, err := ehOpts.AmqpConnection.Sender(electron.Target(eventHubCbsName))
	if err != nil {
		return nil, err
	}
	instance.cbsAmqpLink = cbsAmqpLink

	return instance, nil
}

// -------------------------------------------------

// A Logger can be used simultaneously from multiple go routines;
// it guarantees to serialize access to the Writer.
// cf. https://golang.org/pkg/log/#Logger
var Logger StdLogger = log.New(ioutil.Discard, "[Event Hub]", log.LstdFlags)

// StdLogger is inspired by the Sarama package (a Go Kafka client)
// on how to use the logger from the standard library
type StdLogger interface {
	Printf(format string, v ...interface{})
}
