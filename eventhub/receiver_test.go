package eventhub

import (
	"fmt"
	"net"
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
	"sync"
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

// for i in {1..5}; do go test -v --run TestTheNumberOfAmqpLinksIsTheSameAsThePartitionsOffsets ./eventhub/ ; done
func TestTheNumberOfAmqpLinksIsTheSameAsThePartitionsOffsets(t *testing.T) {
	fakeServer, fakeTCPConn := net.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)
	go func(fakeServer net.Conn) {
		wg.Wait()
		fmt.Printf("Closing the fake server assuming the test has finished already!\n")
		fakeServer.Close()
	}(fakeServer)
	defer fakeTCPConn.Close()

	fakeAmqpConn, _ := electron.NewConnection(fakeTCPConn)
	defer fakeAmqpConn.Close(nil)

	partitionOffsets := []string{"", ""}

	cg, err := newConsumerGroup(consumerGroupOpts{
		eventHubName:      "foo",
		consumerGroupName: "bar",
		partitionOffsets:  partitionOffsets,
		inMsgsChan:        make(chan EhMessage),
		amqpConnection:    fakeAmqpConn,
	})
	if err != nil {
		t.Error(err)
	}

	wg.Done()
	if len(cg.AmqpLinks()) != len(partitionOffsets) {
		t.Error("The number of AMQP links must be equivalent to the number of input partition offsets")
	}
}

func TestPartitionIDIsTheExpectedOne(t *testing.T) {
	// made up input string
	msg := RawMessage{Endpoint: "foo/bar/baz/2"}
	partitionID := msg.ExtractPartitionID()
	if partitionID != 2 {
		t.Errorf("expected partition id to be 2 for input endpoint: '%s'", msg.Endpoint)
	}

	// real input string
	msg = RawMessage{Endpoint: "amqp_receiver_40663@2(<-<EVENT_HUB_NAME>/ConsumerGroups/<CONSUMER_GROUP_NAME>/Partitions/1)"}
	partitionID = msg.ExtractPartitionID()
	if partitionID != 1 {
		t.Errorf("expected partition id to be 1 for input endpoint: '%s'", msg.Endpoint)
	}
}

func TestAmqpFilterMapForOffsets(t *testing.T) {
	inOpts := consumerGroupOpts{
		partitionOffsets: []string{"10", "20"},
	}
	partitionID := 1

	filterMap := amqpMsgFilter(inOpts, partitionID)

	// extract the filter-map
	filterDescription := filterMap[amqp.Symbol("string")].(amqp.Described)

	// check the filter-map descriptor is the expected one
	if filterDescription.Descriptor != fDescriptorAmqpSymbol {
		t.Errorf("The descriptor of the filter map does not have the expected format: %s, found instead: %s", fDescriptorAmqpSymbol, filterDescription.Descriptor)
	}

	// check the filter-map value is the one based on an offset
	expectedFDescriptor := fmt.Sprintf(fOffsetValueTemplate, inOpts.partitionOffsets[partitionID])
	if filterDescription.Value != expectedFDescriptor {
		t.Errorf("The offset value of the filter map does not have the expected format: %s, found instead: %s", expectedFDescriptor, filterDescription.Value)
	}
}

func TestAmqpFilterMapForEnqueuedTime(t *testing.T) {
	nowMillis := time.Now().UnixNano() / int64(time.Millisecond)
	inOpts := consumerGroupOpts{
		epochTimeInMillisec: nowMillis,
	}
	partitionID := 1

	filterMap := amqpMsgFilter(inOpts, partitionID)

	// extract the filter-map
	filterDescription := filterMap[amqp.Symbol("string")].(amqp.Described)

	// check the filter-map descriptor is the expected one
	if filterDescription.Descriptor != fDescriptorAmqpSymbol {
		t.Errorf("The descriptor of the filter map does not have the expected format: %s, found instead: %s", fDescriptorAmqpSymbol, filterDescription.Descriptor)
	}

	// check the filter-map value is the one based on a timestamp in milliseconds
	expectedFDescriptor := fmt.Sprintf(fEnqueuedTimeValueTemplate, nowMillis)
	if filterDescription.Value != expectedFDescriptor {
		t.Errorf("The milliseconds value of the filter map does not have the expected format: %s, found instead: %s", expectedFDescriptor, filterDescription.Value)
	}
}
