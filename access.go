package KJournal

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

//region Record

type Record struct {
	Author  string
	Level   string
	Code    string
	Time    time.Time
	Message string
}

func (r *Record) ToString() string {
	return fmt.Sprintf("(%s) %s (%s:%s) - %s",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Author,
		r.Level,
		r.Code,
		r.Message,
	)
}

func NewRecord(author string, level string, code string, message string) *Record {
	return &Record{
		Author:  author,
		Level:   level,
		Code:    code,
		Time:    time.Now(),
		Message: message,
	}
}

//endregion

//region Reader

//region^ Reader's Container

type ReaderFilter func(*Record) bool

type Reader struct {
	Chan   chan *Record
	Filter ReaderFilter
}

//endregion^

func (kj *KJournal) NewReader(id *string, buf int, filter ReaderFilter) (string, error) {
	if kj.status != KJStatusON {
		return "", fmt.Errorf("KJournal is not running")
	}

	var ID string
	if id == nil {
		ID = uuid.New().String()
	} else {
		ID = *id
	}

	if _, find := kj.readersMap.Load(ID); find {
		return "", fmt.Errorf("Reader with %s already exists", ID)
	}

	if buf <= 0 {
		buf = kj.settings.DefaultReaderBuf
	}
	if filter == nil {
		filter = kj.settings.DefaultReaderFilter
	}

	reader := &Reader{
		Chan:   make(chan *Record, buf),
		Filter: filter,
	}

	kj.readersMap.Store(ID, reader)

	kj.readers.Add(1)
	kj.readersCount.Add(1)

	return ID, nil
}

func (kj *KJournal) DelReader(id string) error {
	if kj.status != KJStatusON && kj.status != KJStatusStop {
		return fmt.Errorf("KJournal is not running")
	}

	if _, loaded := kj.readersMap.LoadAndDelete(id); !loaded {
		return fmt.Errorf("Reader with %s not found", id)
	}

	kj.readers.Done()
	kj.readersCount.Add(-1)

	return nil
}

func (kj *KJournal) AwaitWritersStop() {
	<-kj.ctx.Done()
	if kj.settings.AlarmRWGroup <= 0 {
		kj.writers.Wait()
	} else {
		kj.awaitGroup("writers", &kj.writers, kj.settings.AlarmRWGroup, false)
	}
}

//endregion

//region Writer

func (kj *KJournal) NewWriter() error {
	if kj.status != KJStatusON {
		return fmt.Errorf("KJournal is not running")
	}

	kj.writers.Add(1)
	kj.writersCount.Add(1)

	return nil
}

func (kj *KJournal) DelWriter() error {
	if kj.status != KJStatusON && kj.status != KJStatusStop {
		return fmt.Errorf("KJournal is not running")
	}

	kj.writers.Done()
	kj.writersCount.Add(-1)

	return nil
}

//endregion

//region Read/Write

func (kj *KJournal) Read(id string) (*Record, error) {
	v, find := kj.readersMap.Load(id)
	if !find {
		return nil, fmt.Errorf("Reader with %s not found", id)
	}
	reader := v.(*Reader)

	select {
	case record := <-reader.Chan:
		return record, nil
	default:
		return nil, nil
	}
}

func (kj *KJournal) ReadGroup(id string) ([]*Record, error) {
	v, find := kj.readersMap.Load(id)
	if !find {
		return nil, fmt.Errorf("Reader with %s not found", id)
	}
	reader := v.(*Reader)

	records := make([]*Record, 0, kj.settings.MaxInGroup)
	for i := 0; i < kj.settings.MaxInGroup; i++ {
		select {
		case record := <-reader.Chan:
			records = append(records, record)
		default:
			return records, nil
		}
	}
	return records, nil
}

func (kj *KJournal) Write(record *Record) {
	if record == nil {
		return
	}

	readers := false
	var blocked []string

	kj.readersMap.Range(func(k, v any) bool {
		readers = true
		reader := v.(*Reader)
		if !reader.Filter(record) {
			return true
		}

		if kj.settings.AlarmWrite <= 0 {
			reader.Chan <- record
			return true
		}

		alarm := time.NewTimer(kj.settings.AlarmWrite)
		select {
		case reader.Chan <- record:
			alarm.Stop()
		case <-alarm.C:
			blocked = append(blocked, k.(string))
		}
		return true
	})

	if !readers {
		kj.reportError(NewRecord(
			"Writer",
			"Fatal",
			"Read/Write",
			fmt.Sprintf("write with 0 readers, record lost: %s", record.ToString()),
		))
		return
	}

	if len(blocked) > 0 {
		kj.reportError(NewRecord(
			"Writer",
			"Fatal",
			"WriteTimeout",
			fmt.Sprintf("readers blocked in %s: %v, record lost: %s",
				kj.settings.AlarmWrite, blocked, record.ToString()),
		))
	}
}

//endregion
