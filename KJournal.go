package KJournal

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/admpub/once"
)

//region KJournal's status

type KJStatus int

const (
	KJStatusOFF KJStatus = iota
	KJStatusRunning
	KJStatusStop
	KJStatusON
)

func TranslateStatusToString(s KJStatus) string {
	switch s {
	case KJStatusOFF:
		return "OFF"
	case KJStatusRunning:
		return "Running"
	case KJStatusStop:
		return "Stop"
	case KJStatusON:
		return "ON"
	default:
		return "Unknown"
	}
}

//endregion

//region Structure, settings and constructor

//region^ KJournal's Settings

type KJSettings struct {
	DefaultReaderBuf    int
	DefaultReaderFilter ReaderFilter
	MaxInGroup          int
	FileDumperDir       string
	AlarmRWGroup        time.Duration
	AlarmWrite          time.Duration
}

// desc: Normalizing data and setting default values
func (kjs *KJSettings) normalize() {
	if kjs.DefaultReaderBuf <= 0 {
		kjs.DefaultReaderBuf = 512
	}

	if kjs.DefaultReaderFilter == nil {
		kjs.DefaultReaderFilter = func(r *Record) bool {
			return true
		}
	}

	if kjs.MaxInGroup <= 0 {
		kjs.MaxInGroup = 64
	}
}

//endregion^

//region^ KJournal

type KJournal struct {
	//* Settings and status
	settings KJSettings
	status   KJStatus
	//* Lifecycle
	ctx       context.Context
	cancel    context.CancelFunc
	awaitStop chan struct{}
	stopOnce  once.Once
	//* Readers/Writers
	readers      sync.WaitGroup
	readersCount atomic.Int32
	writers      sync.WaitGroup
	writersCount atomic.Int32
	//* Readers' Map
	readersMap sync.Map
	//* Crash Report
	crashFile *os.File
	crashMu   sync.Mutex
}

func NewKJournal() KJournal {
	return KJournal{}
}

//endregion^

//endregion

//region Lifecycle

//region^ Helper Functions

// desc: Waiting for a group of readers/writers with a timeout
func (kj *KJournal) awaitGroup(name string, group *sync.WaitGroup, timeout time.Duration, report bool) {
	alarm := time.NewTimer(timeout)
	defer alarm.Stop()

	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-alarm.C:
		if report {
			kj.reportError(NewRecord(
				"Lifecycle",
				"Fatal",
				"Readers/Writers",
				fmt.Sprintf("%s not detached in %s, forced stop", name, timeout),
			))
		}
	}
}

//endregion^

func (kj *KJournal) Run(settings KJSettings) error {
	if kj.status != KJStatusOFF {
		return fmt.Errorf("KJournal is already running")
	}
	kj.status = KJStatusRunning

	//* Initialization and check settings
	kj.settings = settings
	kj.settings.normalize()

	//* Initialization lifecycle
	kj.ctx, kj.cancel = context.WithCancel(context.Background())
	kj.awaitStop = make(chan struct{})
	kj.stopOnce.Reset()

	//* Launch of the shutdown mechanism
	go kj.stop()

	kj.status = KJStatusON

	go kj.Dumper()

	return nil
}

func (kj *KJournal) Endwork() error {
	if kj.status != KJStatusON {
		return fmt.Errorf("KJournal is already stopped")
	}

	kj.stopOnce.Do(func() {
		kj.cancel()
		<-kj.awaitStop
	})

	return nil
}

func (kj *KJournal) stop() {
	<-kj.ctx.Done()
	kj.status = KJStatusStop

	//* Await writers and readers
	if kj.settings.AlarmRWGroup <= 0 {
		kj.writers.Wait()
		kj.readers.Wait()
	} else {
		kj.awaitGroup("writers", &kj.writers, kj.settings.AlarmRWGroup, true)
		kj.awaitGroup("readers", &kj.readers, kj.settings.AlarmRWGroup, true)
	}

	kj.closeCrashReport()

	kj.status = KJStatusOFF
	close(kj.awaitStop)
}

//endregion
