package eventhub

import (
	"fmt"
	"os"
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
	"sync/atomic"
	"time"
)

type Sender interface {
	Close()
	Send(message string) (int32, error)
	SendAsync(message string) int32
	SendAsyncTimeout(message string, timeout time.Duration) int32
	ErrorChan() chan error
}

// thread safe counter
type atomicCounter int32

func (ac *atomicCounter) plusOne() int32 {
	return atomic.AddInt32((*int32)(ac), 1)
}
func (ac *atomicCounter) current() int32 {
	return atomic.LoadInt32((*int32)(ac))
}

type sender struct {
	amqpContainer  electron.Container
	amqpConnection electron.Connection
	eventHubAuth   eventHubAuth
	msgLink        electron.Sender
	errorChan      chan error
	outcomeChan    chan electron.Outcome
	msgCounterId   atomicCounter
}

type SenderOpts struct {
	EventHubNamespace   string
	EventHubName        string
	SasPolicyName       string
	SasPolicyKey        string
	TokenExpiryInterval time.Duration
	Debug               bool
}

func setupSendLink(amqpConnection electron.Connection, name string) (electron.Sender, error) {
	// AMQP link to send messages
	msgSender, err := amqpConnection.Sender(electron.Target(name))
	if err != nil {
		return nil, err
	}
	Logger.Printf("Established AMQP link to send messages to the Event Hub: '%v' \n", name)
	return msgSender, nil
}

func (s *sender) asyncPipeOutcomeToError() {
	go func(inS *sender, logger StdLogger) {
		// log.Logger serialzes the access to the writer from multiple go routines,
		// so it should be fine using it here
		// cf. https://golang.org/pkg/log/#Logger
		logger.Printf("Setting up the pipe between the AMQP Outcome (ACK) messages and the Error channel\n")
		for outcomeMsg := range inS.outcomeChan {
			logger.Printf("Received AMQP outcome: %v\n", outcomeMsg)
			err := validateOutcome(outcomeMsg)
			if err != nil {
				logger.Printf("Error '%v' outcome message: %v\n", err, outcomeMsg)
				inS.errorChan <- err
			} else {
				logger.Printf("All good for outcome message: %v\n", outcomeMsg)
			}
		}
	}(s, Logger)
}

// NewSender creates a new AMQP sender to send messages to
// the Azure Event Hub
func NewSender(sendOpts SenderOpts) (Sender, error) {
	// AMQP container
	container := electron.NewContainer(fmt.Sprintf("amqp_sender_%v", os.Getpid()))
	// AMQP connection
	amqpConnection, err := newAmqpConn(container, sendOpts.EventHubNamespace, sendOpts.Debug)
	if err != nil {
		return nil, err
	}

	errorChan := make(chan error)
	// setup the CBS AMQP link and the handshake using the SASL token
	cbsOpts := HandshakeOpts{
		Namespace:            sendOpts.EventHubNamespace,
		Name:                 sendOpts.EventHubName,
		SasPolicyName:        sendOpts.SasPolicyName,
		SasPolicyKey:         sendOpts.SasPolicyKey,
		CbsHandshakeInterval: sendOpts.TokenExpiryInterval,
		AmqpConnection:       amqpConnection,
		ErrorChan:            errorChan,
	}
	eventHubAuth, err := newEventHubAuth(cbsOpts)
	if err != nil {
		return nil, err
	}
	err = eventHubAuth.asyncScheduledHandshake()
	if err != nil {
		return nil, err
	}

	// AMQP link to send messages
	msgSender, err := setupSendLink(amqpConnection, sendOpts.EventHubName)
	if err != nil {
		return nil, err
	}

	Logger.Printf("Returning the sender instance\n")
	senderStruct := sender{
		amqpContainer:  container,
		amqpConnection: amqpConnection,
		msgLink:        msgSender,
		errorChan:      errorChan,
		outcomeChan:    make(chan electron.Outcome),
	}
	senderStruct.asyncPipeOutcomeToError()
	return &senderStruct, nil
}

// Close allows the user to close the AMQP connection
// of the sender instance
func (s *sender) Close() {
	s.amqpConnection.Close(nil)
}

// even though sending/receiving strings straight away works with no issues,
// other Azure services (e.g. Stream Analytics Jobs) expect
// the body of the message to be a byte array (i.e. an encoded string e.g. UTF-8)
// for this reason the message is converted to a byte slice
// calling `SetInferred(bool)`
func (s *sender) prepareAmqpMsg(message string) (amqp.Message, int32) {
	m := amqp.NewMessage()
	m.SetInferred(true)
	m.Marshal([]byte(message))
	curValue := s.msgCounterId.plusOne()
	Logger.Printf("Sending this message: '%s' with id %d \n", message, curValue)
	return m, curValue
}

// Send allows the user to send a message in a synchronous way
func (s *sender) Send(message string) (int32, error) {
	msg, curValue := s.prepareAmqpMsg(message)
	outcome := s.msgLink.SendSync(msg)
	return curValue, validateOutcome(outcome)
}

type valueWrap struct {
	Id int32
}

// SendAsync is a wrapper for the Electron library
// taking care of the new message structure.
// For more details cf.: https://godoc.org/qpid.apache.org/electron#Sender
func (s *sender) SendAsync(message string) int32 {
	msg, curValue := s.prepareAmqpMsg(message)
	s.msgLink.SendAsync(msg, s.outcomeChan, valueWrap{curValue})
	return curValue
}

// SendAsyncTimeout is a wrapper for the Electron library
// taking care of the new message structure.
// For more details cf.: https://godoc.org/qpid.apache.org/electron#Sender
func (s *sender) SendAsyncTimeout(message string, timeout time.Duration) int32 {
	msg, curValue := s.prepareAmqpMsg(message)
	s.msgLink.SendAsyncTimeout(msg, s.outcomeChan, valueWrap{curValue}, timeout)
	return curValue
}

// ErrorChan provides a channel to consume
// any kind of error that could come from this library
func (s *sender) ErrorChan() chan error {
	return s.errorChan
}
