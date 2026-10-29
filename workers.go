package KJournal

import (
	"github.com/google/uuid"
)

//region Readers

// desc: Reader structure
type Reader struct {
	Chan  chan *Record
	Rules func(*Record) bool
}

// desc: registers a reader, returns its id (generated if nil)
func (kj *KJournal) AddReader(id *string, buff int, rules func(*Record) bool) string {
	if rules == nil {
		rules = func(r *Record) bool { return true }
	}
	var ID string
	if id == nil {
		ID = uuid.NewString()
	} else {
		ID = *id
	}
	if _, ok := kj.readers.Load(ID); ok {
		return ID
	}

	reader := Reader{Chan: make(chan *Record, buff), Rules: rules}
	kj.readers.Store(ID, reader)

	kj.readersWait.Add(1)
	kj.readersCount.Add(1)

	return ID
}

// desc: unregisters a reader by id
func (kj *KJournal) DelReader(id string) {
	kj.readers.Delete(id)
	kj.readersCount.Add(-1)
	kj.readersWait.Done()
}

//endregion

//region Writers

// desc: marks a writer as active
func (kj *KJournal) AddWriter() {
	kj.writersWait.Add(1)
	kj.writersCount.Add(1)
}

// desc: marks a writer as done
func (kj *KJournal) DelWriter() {
	kj.writersCount.Add(-1)
	kj.writersWait.Done()
}

//endregion
