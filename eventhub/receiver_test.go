package eventhub

import (
	"fmt"
	"net"
	"qpid.apache.org/electron"
	"testing"
	"time"
)

func TestConstructorForConsumerGroupsIsCheckingTheLengthOfThePartitionOffsetsArray(t *testing.T) {
	_, err := newConsumerGroup(consumerGroupOpts{})
	if err == nil {
		t.Error("The consumer group constructor must validate the partition offsets")
	} else if err.Error() != "A list of partition offsets must be provided, even with defaults to empty strings" {
		t.Error("The validation message for the constructor is not the expected one")
	}
}

func TestTheNumberOfAmqpLinksIsTheSameAsThePartitionsOffsets(t *testing.T) {
	fakeServer, fakeTCPConn := net.Pipe()
	go func() {
		fakeServer.Close()
	}()
	defer fakeTCPConn.Close()

	fakeAmqpConn, _ := electron.NewConnection(fakeTCPConn)
	defer fakeAmqpConn.Close(nil)

	partitionOffsets := []string{"", ""}

	cg, _ := newConsumerGroup(consumerGroupOpts{
		eventHubName:      "foo",
		consumerGroupName: "bar",
		partitionOffsets:  partitionOffsets,
		inMsgsChan:        make(chan EhMessage),
		amqpConnection:    fakeAmqpConn,
	})

	// TODO find out why the following cg.AmqpLinks() randomly fails with:
	// panic: runtime error: invalid memory address or nil pointer dereference
	time.Sleep(2 * time.Second)

	if len(cg.AmqpLinks()) != len(partitionOffsets) {
		t.Error("The number of AMQP links must be equivalent to the number of input partition offsets")
	}
}

func TestPartitionIDIsTheExpectedOne(t *testing.T) {
	// made up input string
	msg := RawMessage{Endpoint: "foo/bar/baz/2"}
	partitionID := msg.ExtractPartitionID()
	if partitionID != 2 {
		t.Error(fmt.Sprintf("expected partition id to be 2 for input endpoint: '%s'", msg.Endpoint))
	}

	// real input string
	msg = RawMessage{Endpoint: "amqp_receiver_40663@2(<-<EVENT_HUB_NAME>/ConsumerGroups/<CONSUMER_GROUP_NAME>/Partitions/1)"}
	partitionID = msg.ExtractPartitionID()
	if partitionID != 1 {
		t.Error(fmt.Sprintf("expected partition id to be 1 for input endpoint: '%s'", msg.Endpoint))
	}
}
