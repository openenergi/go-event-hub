package eventhub

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	defaultRelPath          = "../assets/partition_offsets.csv"
	RecurringTimeLowerBound = 100 * time.Millisecond
)

type OffsetManager interface {
	Current() []string
	UpdateOffset(newValue string, partitionID int)
}

type offsetManager struct {
	// in-memory offsets slice
	mu             sync.Mutex
	currentOffsets []string

	offsetsRelPath     string
	tickerFlushOffsets *time.Ticker
}

type offsetsOpts struct {
	// the size of the slice *MUST* be the same as the number of partitions
	PartitionOffsets []string
	// the CSV file *MUST* contain as many columns as the number of partitions
	PartitionOffsetsPath string
}

// Remember to provide either a slice or a CSV where the number of columns
// *MUST* match the number of partitions setup in the properties of the Event Hub
// on the Azure Portal
func newOffsetManager(pOpts offsetsOpts) (*offsetManager, error) {
	instance := &offsetManager{}

	// if the offsets are in the input options
	// then just return them
	// (and use the CSV path to override that file content,
	// otherwise use the default CSV path)
	if len(pOpts.PartitionOffsets) > 0 {
		instance.currentOffsets = pOpts.PartitionOffsets
		if pOpts.PartitionOffsetsPath == "" {
			Logger.Printf("Using the default path for the CSV partition offsets: %s\n", defaultRelPath)
			instance.offsetsRelPath = defaultRelPath
		} else {
			instance.offsetsRelPath = pOpts.PartitionOffsetsPath
		}
		Logger.Printf("Returning a new instance of the offset manager (based on a provided slice of offsets) with currentOffsets: '%v' and offsetsRelPath: '%v'\n", instance.currentOffsets, instance.offsetsRelPath)
		return instance, nil
	}

	// if there is no path to the partition offsets
	// (and the slice with offsets is not there)
	// then raise an error as we don't know how many
	// partitions are available to return the default value
	if pOpts.PartitionOffsetsPath == "" {
		return nil, errors.New("No input partition offsets, also no path to retrieve that information, there is no way to know the number of partitions")
	}

	// load the offsets
	offsets, err := loadOffsets(pOpts.PartitionOffsetsPath)
	if err != nil {
		return nil, err
	}

	anotherInstance := &offsetManager{
		currentOffsets: offsets,
		offsetsRelPath: pOpts.PartitionOffsetsPath,
	}
	Logger.Printf("Returning a new instance of the offset manager (based on a provided path to a CSV file) with currentOffsets: '%v' and offsetsRelPath: '%v'\n", anotherInstance.currentOffsets, anotherInstance.offsetsRelPath)
	return anotherInstance, nil
}

// Current provides the values for the partitions
// as they are currently stored in-memory
func (om *offsetManager) Current() []string {
	return om.currentOffsets
}

// UpdateOffset could be used to update the value for a given partition ID
func (om *offsetManager) UpdateOffset(newValue string, partitionID int) {
	om.mu.Lock()
	om.currentOffsets[partitionID] = newValue
	Logger.Printf("Done updating the current partition offsets '%v' with the new value '%s' with partitionID '%d'\n", om.currentOffsets, newValue, partitionID)
	om.mu.Unlock()
	// TODO should this return the old value?
}

func (om *offsetManager) concurrentStoreOffsets() {
	om.mu.Lock()
	storeOffsets(om.currentOffsets, om.offsetsRelPath)
	om.mu.Unlock()
}

// A recurring interval no less than 500ms *MUST* be provided,
// otherwise there are too many I/O operations to store the partitions in the CSV file.
func (om *offsetManager) asyncStoreOffsets(recurringTime time.Duration) error {
	if recurringTime < RecurringTimeLowerBound {
		return fmt.Errorf("A recurring interval of less than %s is not valid, please provide a valid recurring interval", RecurringTimeLowerBound)
	}

	om.tickerFlushOffsets = time.NewTicker(recurringTime)
	Logger.Printf("Just setup the async calls to flush the partition offests to file every interval in seconds: %s\n", recurringTime)
	go func(currOm offsetManager) {
		for {
			select {
			case <-currOm.tickerFlushOffsets.C:
				currOm.concurrentStoreOffsets()
			}
		}
	}(*om)
	return nil
}

func loadOffsets(relPath string) ([]string, error) {
	// load the file
	file, err := os.Open(relPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// parse the CSV and validate the content
	lines, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(lines) != 1 {
		return nil, errors.New("The expected format is 1 CSV row containing all the partition offsets")
	}

	// return the array with the partition offsets
	return lines[0], nil
}

func storeOffsets(offsets []string, relPath string) error {
	// load or create the file
	file, err := os.Create(relPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// write the CSV data
	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writer.Write(offsets)
	if err != nil {
		return err
	}

	Logger.Printf("Done flushing to file the current partition offsets '%v' at path %s\n", offsets, relPath)
	return nil
}
