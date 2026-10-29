package KJournal

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/admpub/once"
)

//region Status

// desc: journal lifecycle status
type Status int32

const (
	//* journal is off / not started
	StatusOff Status = iota
	//* journal is starting (transition state)
	StatusRunning
	//* journal is on / accepting work
	StatusOn
	//* journal is stopping (draining)
	StatusStop
)

// desc: human-readable status name
func (s Status) String() string {
	switch s {
	case StatusOff:
		return "OFF"
	case StatusRunning:
		return "RUNNING"
	case StatusOn:
		return "ON"
	case StatusStop:
		return "STOP"
	default:
		return fmt.Sprintf("Status(%d)", int32(s))
	}
}

//endregion

//region Structure

// desc: KJournal structure
type KJournal struct {
	//* Life cycle
	status    Status
	ctx       context.Context
	cancel    context.CancelFunc
	stopOnce  once.Once
	stopAwait chan struct{}
	//* Readers and writers (counters)
	readersWait  sync.WaitGroup
	writersWait  sync.WaitGroup
	readersCount atomic.Int32
	writersCount atomic.Int32
	//* Readers (containers)
	readers sync.Map
}

//endregion

//region Launch

// desc: starts the journal, no-op if already started
func (kj *KJournal) Run() {
	if kj.status != StatusOff {
		return
	}

	defer func() {
		go kj.stop()
	}()

	kj.ctx, kj.cancel = context.WithCancel(context.Background())
	kj.stopOnce.Reset()
	kj.stopAwait = make(chan struct{})

	kj.status = StatusRunning
	kj.status = StatusOn
}

//endregion

//region Stop

// desc: requests graceful shutdown, blocks until drained
func (kj *KJournal) EndWork() {
	if kj.status != StatusOn {
		return
	}
	kj.stopOnce.Do(func() {
		kj.cancel()
		<-kj.stopAwait
	})
}

// desc: internal shutdown routine, drains readers and writers
func (kj *KJournal) stop() {
	<-kj.ctx.Done()

	kj.status = StatusStop
	kj.writersWait.Wait()
	kj.readersWait.Wait()
	kj.status = StatusOff

	close(kj.stopAwait)
}

//endregion
