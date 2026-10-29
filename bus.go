package KJournal

import (
	"fmt"
	"time"
)

//region Record

// desc: Record structure
type Record struct {
	Author  string
	Level   string
	Code    string
	Time    time.Time
	Message string
}

// desc: Record conversion according to the accepted standard
func (r *Record) ToString() string {
	return fmt.Sprintf("%s [%s] (%s) %s: %s",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Level,
		r.Code,
		r.Author,
		r.Message,
	)
}

// desc: Record constructor
func NewRecord(author, level, code, message string) *Record {
	return &Record{
		Author:  author,
		Level:   level,
		Code:    code,
		Time:    time.Now(),
		Message: message,
	}
}

//endregion

//region Reading and writing

//region^ Reading

// desc: non-blocking read from a reader by id
func (kj *KJournal) Read(id string) (*Record, bool) {
	v, ok := kj.readers.Load(id)
	if !ok {
		return nil, false
	}
	ch := v.(Reader)

	select {
	case record, ok := <-ch.Chan:
		return record, ok
	default:
		return nil, false
	}
}

//endregion^

//region^ Writing

// desc: broadcasts record to all matching readers
func (kj *KJournal) Write(record *Record) {
	kj.readers.Range(func(_, v any) bool {
		ch := v.(Reader)
		if ch.Rules(record) {
			ch.Chan <- record
		}
		return true
	})
}

//endregion^

//endregion
