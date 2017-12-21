package eventhub

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestCsvLoadForValidCsvFile(t *testing.T) {
	offsets, err := loadOffsets(defaultRelPath)
	if err != nil {
		t.Error(err)
	}
	if len(offsets) != 2 {
		t.Errorf("The input CSV file is supposed to have 1 row with 2 fields, but it has instead %d fields!", len(offsets))
	}
}

func TestCsvLoadForInvalidCsvFile(t *testing.T) {
	_, err := loadOffsets("../assets/test_invalid_format.csv")

	if err == nil || fmt.Sprint(err) != "The expected format is 1 CSV row containing all the partition offsets" {
		t.Error("Unexpected error behaviour for an invalid CSV with more than 1 row")
	}
}

func TestCsvWriteThenRead(t *testing.T) {
	// the CSV file will be written, read, checked, then deleted at the end of the test
	relPath := "../assets/test_file.csv"
	expectedOffsets := []string{"foo", "bar"}

	err := storeOffsets(expectedOffsets, relPath)
	if err != nil {
		t.Error("Unexpected error when storing the CSV file")
	}

	foundOffsets, err := loadOffsets(relPath)
	if err != nil {
		t.Error(err)
	}
	if (len(foundOffsets) != 2) ||
		(len(expectedOffsets) != 2) ||
		(foundOffsets[0] != expectedOffsets[0]) ||
		(foundOffsets[1] != expectedOffsets[1]) {
		t.Errorf("Found the offsets '%v' but expected to find '%v'", foundOffsets, expectedOffsets)
	}

	os.Remove(relPath)
}

func TestCsvWriteThenReadNumbersAsStrings(t *testing.T) {
	// the CSV file will be written, read, checked, then deleted at the end of the test
	relPath := "../assets/test_file.csv"
	expectedOffsets := []string{"1234", "5678"}

	err := storeOffsets(expectedOffsets, relPath)
	if err != nil {
		t.Error("Unexpected error when storing the CSV file")
	}

	foundOffsets, err := loadOffsets(relPath)
	if err != nil {
		t.Error(err)
	}
	if (len(foundOffsets) != 2) ||
		(len(expectedOffsets) != 2) ||
		(foundOffsets[0] != expectedOffsets[0]) ||
		(foundOffsets[1] != expectedOffsets[1]) {
		t.Errorf("Found the offsets '%v' but expected to find '%v'", foundOffsets, expectedOffsets)
	}

	os.Remove(relPath)
}

func TestOffsetManagerDefaultValueForPartitonOffsetsWhenPassingThemAsASlice(t *testing.T) {
	defaultOffsets := []string{"", "", ""}
	om, err := newOffsetManager(offsetsOpts{PartitionOffsets: defaultOffsets})
	if err != nil {
		t.Error(err)
	}

	currentPartitonOffsets := om.Current()

	if len(defaultOffsets) != len(currentPartitonOffsets) {
		t.Errorf("The expected length of partitions '%d' is different: '%d'", len(defaultOffsets), len(currentPartitonOffsets))
	}

	if defaultOffsets[0] != currentPartitonOffsets[0] ||
		defaultOffsets[1] != currentPartitonOffsets[1] ||
		defaultOffsets[2] != currentPartitonOffsets[2] {
		t.Errorf("The expected values for the partitions '%v' are different than the found ones: '%v'", defaultOffsets, currentPartitonOffsets)
	}
}

func TestOffsetManagerDefaultValueForPartitonOffsetsWhenPassingThemAsAPath(t *testing.T) {
	om, err := newOffsetManager(offsetsOpts{PartitionOffsetsPath: defaultRelPath})
	if err != nil {
		t.Error(err)
	}

	currentPartitonOffsets := om.Current()

	if len(currentPartitonOffsets) != 2 {
		t.Errorf("The expected length of partitions (2) at path '%s' is different: '%d'", defaultRelPath, len(currentPartitonOffsets))
	}

	if currentPartitonOffsets[0] != "" ||
		currentPartitonOffsets[1] != "" {
		t.Errorf("The expected values for the partitions at path '%v' are different than the found ones: '%v'", defaultRelPath, currentPartitonOffsets)
	}
}

func TestOffsetManagerUpdate(t *testing.T) {
	om, err := newOffsetManager(offsetsOpts{PartitionOffsetsPath: defaultRelPath})
	if err != nil {
		t.Error(err)
	}
	newOffset := "foo"
	partitionID := 1
	om.UpdateOffset(newOffset, partitionID)
	updatedPartitonOffsets := om.Current()

	if updatedPartitonOffsets[partitionID] != newOffset {
		t.Errorf("The expected values for the partitions are different than the found ones: '%v'", updatedPartitonOffsets)
	}
}

func TestAsyncFlushOfTheOffsets(t *testing.T) {
	// async update of the in-memory offsets every 2 seconds
	updateOffsetInterval := 2 * time.Second
	// async storage of the offsets every 6 seconds
	flushInterval := 3 * updateOffsetInterval
	// wait before asserting on the CSV file for 12 seconds
	checkFlushedOffsetsInterval := 2 * flushInterval
	// the offsets to be updated at each "update call" (12/2=6 times in total)
	offsets := []string{"foo", "bar", "baz", "xxx", "yyy", "zzz"}
	partitionIDToUpdate := 0
	partitionIDNoUpdate := 1
	partitionOffsetsPath := "../assets/test_offsets_lifecycle.csv"

	om, err := newOffsetManager(offsetsOpts{PartitionOffsetsPath: partitionOffsetsPath})
	if err != nil {
		t.Error(err)
	}

	// async update map every 2 seconds
	go func(currOm *offsetManager, interval time.Duration) {
		tickerUpdateOffsets := time.NewTicker(interval)
		currIndex := 0
		for {
			select {
			case <-tickerUpdateOffsets.C:
				fmt.Printf("The ticker ticked! The currIndex is: %d\n", currIndex)
				currOm.UpdateOffset(offsets[currIndex], partitionIDToUpdate)
				currIndex++
			}
			if currIndex == len(offsets) {
				break
			}
		}
		return
	}(om, updateOffsetInterval)

	// wait just a bit in order to put out of phase the async storage of the offsets
	time.Sleep(500 * time.Millisecond)
	// async every 6 seconds
	om.asyncStoreOffsets(flushInterval)

	// async assert every update of the in-memory map
	go func(currOm *offsetManager, interval time.Duration, t *testing.T) {
		tickerCheckOffsets := time.NewTicker(interval)
		currIdx := 0
		for {
			select {
			case <-tickerCheckOffsets.C:
				currOffsets := currOm.Current()
				expectedOffset := offsets[currIdx]
				hasUnaffectedPartitionChangedOffset := currOffsets[partitionIDNoUpdate] != ""
				hasAffectedPartitionUnexpectedOffset := currOffsets[partitionIDToUpdate] != expectedOffset
				fmt.Printf("Integration test: checking current index %d (and expected offset %s) and current offsets %v. hasUnaffectedPartitionChangedOffset? %v hasAffectedPartitionUnexpectedOffset? %v \n", currIdx, expectedOffset, currOffsets, hasUnaffectedPartitionChangedOffset, hasAffectedPartitionUnexpectedOffset)
				if hasUnaffectedPartitionChangedOffset ||
					hasAffectedPartitionUnexpectedOffset {
					panic(fmt.Sprintf("Something weird with the in-memory values: '%v' hasUnaffectedPartitionChangedOffset? %v hasAffectedPartitionUnexpectedOffset? %v \n", currOffsets, hasUnaffectedPartitionChangedOffset, hasAffectedPartitionUnexpectedOffset))
				}
				currIdx++
			}
			if currIdx == len(offsets) {
				break
			}
		}
		return
	}(om, updateOffsetInterval, t)

	// after 12 seconds (+ 1.5 seconds of phase) load the offsets from file...
	time.Sleep(checkFlushedOffsetsInterval + 1500*time.Millisecond)
	// ...and assert
	foundOffsets, err := loadOffsets(partitionOffsetsPath)
	if err != nil {
		t.Error(err)
	}
	if foundOffsets[partitionIDToUpdate] != offsets[5] {
		t.Errorf("Async flushing issues, the CSV file was: '%v', it's not containing: '%s'", foundOffsets, offsets[5])
	}
	if foundOffsets[1] != "" {
		t.Errorf("Async flushing issues, the CSV file was: '%v', it's not containing empty values for the not-updated fields", foundOffsets)
	}

}

func TestAsyncShouldValidateTheTimeInterval(t *testing.T) {
	om, err := newOffsetManager(offsetsOpts{PartitionOffsetsPath: defaultRelPath})
	if err != nil {
		t.Error(err)
	}

	intervalLessThan100ms := 99 * time.Millisecond
	err = om.asyncStoreOffsets(intervalLessThan100ms)
	if err == nil {
		t.Errorf("An error is expected for intervals less than 100ms, the interval was: %s\n", intervalLessThan100ms)
	}
}
