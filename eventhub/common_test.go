package eventhub

import (
	"fmt"
	"os"
	"qpid.apache.org/electron"
	"testing"
	"time"
)

const (
	ehNamespace         = "<FILL_THIS_WITH_THE_RELEVANT_DETAILS>"
	ehName              = "<FILL_THIS_WITH_THE_RELEVANT_DETAILS>"
	superUserPolicy     = "<FILL_THIS_WITH_THE_RELEVANT_DETAILS>"
	superUserPrimaryKey = "<FILL_THIS_WITH_THE_RELEVANT_DETAILS>"
)

func TestCbsLinkConstructor(t *testing.T) {
	// AMQP container
	amqpContainer := electron.NewContainer(fmt.Sprintf("amqp_receiver_%v", os.Getpid()))
	// AMQP connection
	amqpConnection, err := newAmqpConn(amqpContainer, ehNamespace, true)
	if err != nil {
		t.Error(err)
	}
	cbsOpts := HandshakeOpts{
		Namespace:            ehNamespace,
		Name:                 ehName,
		SasPolicyName:        superUserPolicy,
		SasPolicyKey:         superUserPrimaryKey,
		CbsHandshakeInterval: 20 * time.Second,
		AmqpConnection:       amqpConnection,
	}

	// the integration test starts here with the Microsoft CBS logic
	// about the SASL token and the handshake
	eventHubAuth, err := newEventHubAuth(cbsOpts)
	if err != nil {
		t.Error(err)
	}
	err = eventHubAuth.asyncScheduledHandshake()
	if err != nil {
		t.Error(err)
	}
}

func TestLengthWhenReadingSlice(t *testing.T) {
	zeroRand := newZeroRand()
	firstNum, err := zeroRand.Read([]byte("foo"))
	if err != nil {
		t.Error(err)
	}
	secondNum, err := zeroRand.Read([]byte("bar"))
	if err != nil {
		t.Error(err)
	}

	if firstNum != secondNum {
		t.Error(fmt.Sprintf("This is not a predictable zero random generator! The first number is: %d the second number is: %d", firstNum, secondNum))
	}
}

func TestSliceResetWhenInvokingRead(t *testing.T) {
	zeroRand := newZeroRand()
	inputBytes := []byte("foo")
	copyInputBytes := make([]byte, len(inputBytes))
	copy(copyInputBytes, inputBytes)

	lenInputBytes, err := zeroRand.Read(inputBytes)
	if err != nil {
		t.Error(err)
	}

	if lenInputBytes != len(inputBytes) {
		t.Error("Length mismatch!")
	}

	for idx, elem := range inputBytes {
		if copyInputBytes[idx] == inputBytes[idx] {
			t.Error("The input slice should be modified by the Read method!")
		}
		if elem != byte(0) {
			t.Error("The slice should be modified and contain only zeros!")
		}
	}
	// fmt.Printf("firstNum %d\n", firstNum)
	// fmt.Printf("inputBytes %d\n", inputBytes)
}
