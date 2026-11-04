package KJournal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

//region Metrics

type KJMetrics struct {
	Status      string
	Writers     int
	Readers     int
	ReadersFill map[string]struct{ Len, Cap int }
}

func (kj *KJournal) GetMetrics() KJMetrics {
	m := KJMetrics{
		Status:  TranslateStatusToString(kj.status),
		Writers: int(kj.writersCount.Load()),
		Readers: int(kj.readersCount.Load()),
		ReadersFill: map[string]struct {
			Len int
			Cap int
		}{},
	}

	kj.readersMap.Range(func(k, v any) bool {
		id := k.(string)
		reader := v.(*Reader)
		m.ReadersFill[id] = struct {
			Len int
			Cap int
		}{
			Len: len(reader.Chan),
			Cap: cap(reader.Chan),
		}
		return true
	})

	return m
}

//endregion

//region Records Dumper in dir

//region^ Helper Functions

func (kj *KJournal) dumperCrashError(code, message string) {
	kj.reportError(NewRecord("Dumper", "Fatal", code, message))
}

// desc: Preparing and validating the path to the future dumps directory
func (kj *KJournal) prepareDumperDir() error {
	dir := kj.settings.FileDumperDir

	if !filepath.IsAbs(dir) {
		exePath, err := os.Executable()
		if err != nil {
			return fmt.Errorf("dumper dir: get executable path: %w", err)
		}
		dir = filepath.Join(filepath.Dir(exePath), dir)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("dumper dir: create %q: %w", dir, err)
	}

	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("dumper dir: stat %q: %w", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("dumper dir: %q is not a directory", dir)
	}

	probe := filepath.Join(dir, ".kj_probe")
	f, err := os.OpenFile(probe, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("dumper dir: not writable %q: %w", dir, err)
	}
	_ = f.Close()
	_ = os.Remove(probe)

	kj.settings.FileDumperDir = dir
	return nil
}

// desc: Flushing accumulated records to a file
func (kj *KJournal) dumpToFile(buff chan *Record) {
	dir := kj.settings.FileDumperDir
	if dir == "" {
		return
	}

	name := time.Now().Format("2006-01-02_15-04-05.000000000") + ".kj"
	path := filepath.Join(dir, name)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		kj.dumperCrashError("FileSystem", fmt.Sprintf("dump file: create %q: %v\n", path, err))
		return
	}
	defer func() {
		if errC := f.Close(); errC != nil {
			kj.dumperCrashError("FileSystem", fmt.Sprintf("dump file: close %q: %v\n", path, errC))
		}
	}()

	for {
		select {
		case record := <-buff:
			if record == nil {
				continue
			}
			if _, errW := fmt.Fprintln(f, record.ToString()); errW != nil {
				kj.dumperCrashError("FileSystem", fmt.Sprintf("dump file: write %q: %v\n", path, errW))
				return
			}
		default:
			return
		}
	}
}

//endregion^

func (kj *KJournal) Dumper() {
	if kj.settings.FileDumperDir == "" {
		return
	}
	if err := kj.prepareDumperDir(); err != nil {
		kj.dumperCrashError("FileSystem", fmt.Sprintf("prepareDumperDir: %v", err))
		return
	}

	//* Launching a reader to collect records
	dumperID := "KJDumper"
	id, errNR := kj.NewReader(&dumperID, 0, nil)
	if errNR != nil {
		kj.dumperCrashError("Reader", fmt.Sprintf("NewReader: %v", errNR))
		return
	}
	defer func() {
		if errDR := kj.DelReader(id); errDR != nil {
			kj.dumperCrashError("Reader", fmt.Sprintf("DelReader: %v", errDR))
		}
	}()

	//* Record-collecting goroutine
	DumperBuffer := make(chan *Record, kj.settings.MaxInGroup*3)
	ctxDumper, cancelDumper := context.WithCancel(context.Background())
	go func() {
		kj.AwaitWritersStop()
		cancelDumper()
	}()
	for {
		select {
		case <-ctxDumper.Done():
			kj.dumpToFile(DumperBuffer)
			return
		default:
			records, errR := kj.ReadGroup(id)
			if errR != nil {
				return
			}

			for _, record := range records {
				select {
				case DumperBuffer <- record:
				default:
					kj.dumpToFile(DumperBuffer)
					DumperBuffer <- record
				}
			}
		}
	}
}

//endregion

//region Crash Report

func (kj *KJournal) createCrashReport() {
	exePath, err := os.Executable()
	if err != nil {
		panic(fmt.Sprintf("KJournal: crash report: get executable: %v", err))
	}

	path := filepath.Join(filepath.Dir(exePath), "crashReport.kj")

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		panic(fmt.Sprintf("KJournal: crash report: create %q: %v", path, err))
	}

	kj.crashFile = f
}

func (kj *KJournal) closeCrashReport() {
	if kj.crashFile == nil {
		return
	}
	_ = kj.crashFile.Close()
	kj.crashFile = nil
}

func (kj *KJournal) reportError(record *Record) {
	if record == nil {
		return
	}

	kj.crashMu.Lock()
	defer kj.crashMu.Unlock()

	if kj.crashFile == nil {
		kj.createCrashReport()
	}

	_, _ = fmt.Fprintln(kj.crashFile, record.ToString())
}

//endregion
