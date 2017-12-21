package eventhub

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Receiver interface {
	Close()
	AsyncFetch()
	ReceiveChan() chan EhMessage
	ErrorChan() chan error
}

type receiver struct {
	amqpContainer  electron.Container
	amqpConnection electron.Connection
	eventHubAuth   *eventHubAuth
	offsetManager  *offsetManager
	receiveChan    chan EhMessage
	consumerGroup  *consumerGroup
	errorChan      chan error
}

type ReceiverOpts struct {
	EventHubNamespace    string
	EventHubName         string
	SasPolicyName        string
	SasPolicyKey         string
	ConsumerGroupName    string
	LinkCapacity         int
	PartitionOffsets     []string
	PartitionOffsetsPath string
	OffsetsFlushInterval time.Duration
	TokenExpiryInterval  time.Duration
	Debug                bool
}

// Every time a new incoming message is processed
// the in-memory slice with the partition offsets
// is updated
func asyncPipeChannelsAndUpdateInMemoryOffsets(cgBackendChan chan EhMessage, libraryFrontendChan chan EhMessage, om offsetManager) {
	go func(currOm offsetManager, cgBackendChan chan EhMessage, libraryFrontendChan chan EhMessage) {
		for currEhMsg := range cgBackendChan {
			Logger.Printf("The received msg (to update the in-memory offsets): %v\n", currEhMsg)
			currOm.UpdateOffset(currEhMsg.Offset, currEhMsg.PartitionId)
			libraryFrontendChan <- currEhMsg
		}
	}(om, cgBackendChan, libraryFrontendChan)
}

// NewReceiver returns a new receiver connected to the Azure Event Hub
// specified in the input ReceiverOpts struct
func NewReceiver(recOpts ReceiverOpts) (Receiver, error) {
	// AMQP container
	amqpContainer := electron.NewContainer(fmt.Sprintf("amqp_receiver_%v", os.Getpid()))
	// AMQP connection
	amqpConnection, err := newAmqpConn(amqpContainer, recOpts.EventHubNamespace, recOpts.Debug)
	if err != nil {
		return nil, err
	}

	errorChan := make(chan error)
	// setup the CBS AMQP link and the handshake using the SASL token
	cbsOpts := handshakeOpts{
		Namespace:            recOpts.EventHubNamespace,
		Name:                 recOpts.EventHubName,
		SasPolicyName:        recOpts.SasPolicyName,
		SasPolicyKey:         recOpts.SasPolicyKey,
		CbsHandshakeInterval: recOpts.TokenExpiryInterval,
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

	// Channel where the messages will be published to the user of this library
	cgOutChan := make(chan EhMessage)
	// Channel where the AMQP messages from the Event Hub will be published
	msgOutChan := make(chan EhMessage)
	// CSV file storage for the partition offsets
	offsetManager, err := newOffsetManager(offsetsOpts{PartitionOffsets: recOpts.PartitionOffsets, PartitionOffsetsPath: recOpts.PartitionOffsetsPath})
	if err != nil {
		return nil, err
	}
	// Consumer Group to receive messages from different partitions of the Event Hub
	cg, err := newConsumerGroup(consumerGroupOpts{
		eventHubName:      recOpts.EventHubName,
		consumerGroupName: recOpts.ConsumerGroupName,
		partitionOffsets:  offsetManager.Current(),
		inMsgsChan:        cgOutChan,
		amqpConnection:    amqpConnection,
		linkCapacity:      recOpts.LinkCapacity,
	})
	if err != nil {
		return nil, err
	}

	// First: start the async process to flush the partition offsets to the CSV file
	err = offsetManager.asyncStoreOffsets(recOpts.OffsetsFlushInterval)
	if err != nil {
		return nil, err
	}
	// Second: asynchronously pipe the received messages "EhMessage" between
	// 2 equivalent "backend channel" and "frontend channel" in order
	// to be able to update the Partition (offsets to be stored in a CSV file)
	asyncPipeChannelsAndUpdateInMemoryOffsets(cgOutChan, msgOutChan, *offsetManager)

	Logger.Printf("Returning the receiver instance\n")
	return &receiver{
		amqpContainer:  amqpContainer,
		amqpConnection: amqpConnection,
		eventHubAuth:   eventHubAuth,
		offsetManager:  offsetManager,
		receiveChan:    msgOutChan,
		errorChan:      errorChan,
		consumerGroup:  cg,
	}, nil
}

func (r *receiver) Close() {
	r.amqpConnection.Close(nil)
}

// AsyncFetch spawns the inner consumer group AMQP links.
// The inner consumer groups will publish the messages
// in the channel specified when creating the AMQP receiver
func (r *receiver) AsyncFetch() {
	r.consumerGroup.SpawnCgAmqpLinksConsumers()
}

// ReceiveChan provides a channel to consume from the
// messages coming from the Azure Event Hub
func (r *receiver) ReceiveChan() chan EhMessage {
	return r.receiveChan
}

// ErrorChan provides a channel to consume
// any kind of error that could come from this library
func (r *receiver) ErrorChan() chan error {
	return r.errorChan
}

// ---------------------------------------------------

// RawMessage contains the raw fields coming from the AMQP connection.
// No information has been extracted from them.
type RawMessage struct {
	AmqpMsg  amqp.Message `json:"amqp_msg"`
	Endpoint string       `json:"endpoint"`
}

// EhMessage is the struct containing all the useful information
// provided by the Azure Event Hub. Body is the main field, containing
// the body of the message, but there are other interesting fields
// regarding the Partition offset, when the message was enqueued,
// along with other details about the Event Hub partitions.
type EhMessage struct {
	Body           string    `json:"body"`
	SequenceNumber int64     `json:"sequence_number"`
	PartitionKey   string    `json:"partiton_key"`
	Offset         string    `json:"offset"`
	EnqueuedTime   time.Time `json:"enqueued_time"`
	EhEndpoint     string    `json:"eh_endpoint"`
	PartitionId    int       `json:"partition_id"`
}

var (
	amqpEhOptSequenceNumberKey = amqp.AnnotationKeyString("x-opt-sequence-number")
	amqpEhOptPartitionKeyKey   = amqp.AnnotationKeyString("x-opt-partition-key")
	amqpEhOptOffsetKey         = amqp.AnnotationKeyString("x-opt-offset")
	amqpEhOptEnqueuedTimeKey   = amqp.AnnotationKeyString("x-opt-enqueued-time")
)

// ToEhMessage transforms a RawMessage to a EhMessage
func (msg RawMessage) ToEhMessage() EhMessage {
	output := EhMessage{}
	output.EhEndpoint = msg.Endpoint
	output.PartitionId = msg.ExtractPartitionId()
	// allowing both message bodies of type 'string' and '[]byte'
	// TODO check if the AMQP standard allows messages with bodies of different type
	// in the same AMQP link/connection
	switch msg.AmqpMsg.Body().(type) {
	case amqp.Binary:
		output.Body = string([]byte(msg.AmqpMsg.Body().(amqp.Binary)))
	case string:
		output.Body = msg.AmqpMsg.Body().(string)
	default:
		panic(fmt.Sprintf("Unexpected type for the AMQP message, this Event Hub library allows to send messages only as 'string' or '[]byte', now the received message has type: '%v'", reflect.TypeOf(msg.AmqpMsg.Body()).Kind()))
	}
	annotationsMap := msg.AmqpMsg.MessageAnnotations()
	output.SequenceNumber = annotationsMap[amqpEhOptSequenceNumberKey].(int64)
	output.Offset = annotationsMap[amqpEhOptOffsetKey].(string)
	output.EnqueuedTime = annotationsMap[amqpEhOptEnqueuedTimeKey].(time.Time)
	partitionKeyValue := annotationsMap[amqpEhOptPartitionKeyKey]
	if partitionKeyValue != nil {
		output.PartitionKey = partitionKeyValue.(string)
	}

	return output
}

// ExtractPartitionId gets the partition number from the RawMessage
// finding it from the Endpoint field
func (msg RawMessage) ExtractPartitionId() int {
	// "amqp_receiver_40663@2(<-<EVENT_HUB_NAME>/ConsumerGroups/<CONSUMER_GROUP_NAME>/Partitions/<PARTITION_ID>)"
	arr := strings.Split(msg.Endpoint, "/")
	// "1)" -> "1"
	numStr := strings.Split(arr[len(arr)-1], ")")[0]
	// 1
	num, _ := strconv.Atoi(numStr)
	return num
}

// String is the string (JSON) representation of a RawMessage
func (rawMsg RawMessage) String() string {
	jsonMsg, _ := json.Marshal(rawMsg)
	return string(jsonMsg)
}

// String is the string (JSON) representation of a EhMessage
func (msg EhMessage) String() string {
	jsonMsg, _ := json.Marshal(msg)
	return string(jsonMsg)
}

// ---------------------------------------------------

type consumerGroup struct {
	inMsgsChan chan EhMessage
	amqpLinks  []electron.Receiver
}

type consumerGroupOpts struct {
	eventHubName      string
	consumerGroupName string
	partitionOffsets  []string
	inMsgsChan        chan EhMessage
	amqpConnection    electron.Connection
	linkCapacity      int
}

func amqpMsgFilter(cgOpts consumerGroupOpts, partitionId int) map[amqp.Symbol]interface{} {
	filterMap := make(map[amqp.Symbol]interface{})
	partitionOffset := cgOpts.partitionOffsets[partitionId]
	if partitionOffset != "" {
		offsetDesc := amqp.Described{
			Descriptor: amqp.Symbol("apache.org:selector-filter:string"),
			Value:      fmt.Sprintf("amqp.annotation.x-opt-offset > '%v'", partitionOffset),
		}
		filterMap[amqp.Symbol("string")] = offsetDesc
		Logger.Printf("The filter map for partitionId %d is: %v\n", partitionId, filterMap)
		return filterMap
	}
	Logger.Printf("No offset for partitionId %d, returning nil\n", partitionId)
	return nil
}

const defaultLinkCapacity = 50

// This returns an electron.Receiver instance
func amqpLink(cgOpts consumerGroupOpts, filterMap map[amqp.Symbol]interface{}, partitionId int) (electron.Receiver, error) {
	// build the consumer group path
	cgPath := fmt.Sprintf("%s/ConsumerGroups/%s/Partitions/%d", cgOpts.eventHubName, cgOpts.consumerGroupName, partitionId)
	// validate the link capacity parameter
	if cgOpts.linkCapacity == 0 {
		cgOpts.linkCapacity = defaultLinkCapacity
		Logger.Printf("Using the default link capacity of: %d\n", defaultLinkCapacity)
	}

	Logger.Printf("AMQP receiver link path: '%s' and capacity %v \n", cgPath, cgOpts.linkCapacity)
	if filterMap != nil {
		return cgOpts.amqpConnection.Receiver(
			electron.Source(cgPath),
			electron.Capacity(cgOpts.linkCapacity),
			electron.Prefetch(true),
			electron.Filter(filterMap),
		)
	}
	return cgOpts.amqpConnection.Receiver(
		electron.Source(cgPath),
		electron.Capacity(cgOpts.linkCapacity),
		electron.Prefetch(true),
	)
}

func createAllAmqpLinks(cgOpts consumerGroupOpts) ([]electron.Receiver, error) {
	amqpLinks := make([]electron.Receiver, len(cgOpts.partitionOffsets))
	for pIdx := 0; pIdx < len(cgOpts.partitionOffsets); pIdx++ {
		filterMap := amqpMsgFilter(cgOpts, pIdx)
		ehNodeReceiver, err := amqpLink(cgOpts, filterMap, pIdx)
		if err != nil {
			return nil, err
		}
		amqpLinks[pIdx] = ehNodeReceiver
	}
	Logger.Printf("Done creating the AMQP links with offsets %v for the partitions of the consumer group, the list has size %v\n", cgOpts.partitionOffsets, len(amqpLinks))
	return amqpLinks, nil
}

// AmqpLinks returns the current list of AMQP links
// belonging to the consumer group
func (cg *consumerGroup) AmqpLinks() []electron.Receiver {
	return cg.amqpLinks
}

// SpawnCgAmqpLinksConsumers initializes the message retrieval
// for all the AMQP links in their own dedicated go routine
func (cg *consumerGroup) SpawnCgAmqpLinksConsumers() {
	Logger.Printf("Reading the links from a list of size: %v\n", len(cg.amqpLinks))
	for _, currAmqpLink := range cg.amqpLinks {
		Logger.Printf("Spawning readings from AMQP link '%v'\n", currAmqpLink)
		go func(amqpLink electron.Receiver) {
			for {
				if receivedMsg, err := amqpLink.Receive(); err == nil {
					rawMsg := RawMessage{AmqpMsg: receivedMsg.Message, Endpoint: amqpLink.String()}
					ehMsg := rawMsg.ToEhMessage()
					Logger.Printf("This AMQP link: '%v', received a message, the transformed to send is: %v\n", rawMsg.Endpoint, ehMsg)
					cg.inMsgsChan <- ehMsg
					err := receivedMsg.Accept()
					if err != nil {
						Logger.Printf("WARN: %v\n", err)
					}
				} else if err == electron.Closed {
					Logger.Printf("It seems the connection has been closed for AMQP link: '%s'\n", amqpLink)
				} else {
					Logger.Printf("Issues for AMQP link '%v', the error is: %v\n", amqpLink, err)
				}

				// enable this to slow down the message rate
				// while debugging/troubleshooting the receiver
				// time.Sleep(2 * time.Second)
			}
		}(currAmqpLink)
	}
}

func newConsumerGroup(cgOpts consumerGroupOpts) (*consumerGroup, error) {
	if len(cgOpts.partitionOffsets) == 0 {
		// the offsets must be set for each partition
		// (empty strings "" are the default to retrieve from the beginning of time)
		return nil, errors.New("A list of partition offsets must be provided, even with defaults to empty strings")
	}
	amqpLinks, err := createAllAmqpLinks(cgOpts)
	if err != nil {
		return nil, err
	}
	return &consumerGroup{
		inMsgsChan: cgOpts.inMsgsChan,
		amqpLinks:  amqpLinks,
	}, nil
}
