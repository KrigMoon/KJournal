# KJournal

**KJournal** is an in-memory journal. Writers add `Record` entries, and readers receive them through their own channels, filtered by `ReaderFilter`. You can set timeouts, default values, and a folder for saving records to files. Errors and lost records are written to `crashReport.kj`, and the current state is available through `GetMetrics`.

## Settings

| Name | Type | Default | Description |
|---|---|---|---|
| `DefaultReaderBuf` | `int` | `512` | Default reader buffer size |
| `DefaultReaderFilter` | `ReaderFilter` | pass all records | Default reader filter |
| `MaxInGroup` | `int` | `64` | Maximum records per single group read |
| `FileDumperDir` | `string` | empty (dumper disabled) | Folder for saving records to files |
| `AlarmRWGroup` | `time.Duration` | `0` (no timeout) | Timeout for waiting on reader and writer groups |
| `AlarmWrite` | `time.Duration` | `0` (no timeout) | Timeout for writing to a reader channel |

## Lifecycle

The journal goes through the statuses `OFF`, `Running`, `Stop`, and `ON`. Startup moves it from `OFF` to `ON`, and shutdown moves it back to `OFF`. Starting an already running journal or stopping an already stopped one returns an error.

### Startup

Startup is performed by the `Run` method and takes `KJSettings`. The method works only from the `OFF` status; in any other status it returns the error "KJournal is already running". Before start, the settings are normalized: empty and zero values are replaced with defaults. Then the context and service fields are created, and the internal shutdown goroutine is started. After that the status changes to `ON`, and the journal is ready to work.

### Shutdown

Shutdown is performed by the `Endwork` method. The method works only from the `ON` status; in any other status it returns the error "KJournal is already stopped". Shutdown is idempotent: a repeated call does nothing. Inside, the context is canceled and the internal shutdown goroutine is awaited. The goroutine waits for writers and readers - with or without the `AlarmRWGroup` timeout. Then `crashReport.kj` is closed, and the status changes to `OFF`.

## Entities

### Record

`Record` is a single journal entry. It is created via `NewRecord`, and the time is set automatically. The `ToString` method returns a text representation of the record.

| Name | Type | Description |
|---|---|---|
| `Author` | `string` | Record author |
| `Level` | `string` | Record level |
| `Code` | `string` | Record code |
| `Time` | `time.Time` | Creation time |
| `Message` | `string` | Message text |

API:
- `NewRecord(author, level, code, message string) *Record` - creates a record;
- `(*Record).ToString() string` - returns a text representation.

### Reader

> **Warning**
>
> The reader must be created before potential writers, otherwise it will miss records sent before its creation.

`Reader` is a record reader. It has its own channel and filter. It is created via `NewReader` and removed via `DelReader`.

| Name | Type | Description |
|---|---|---|
| `Chan` | `chan *Record` | Channel that receives records |
| `Filter` | `ReaderFilter` | Filter: `true` means the record matches the reader |

> `ReaderFilter` is `func(*Record) bool`.

API:
- `NewReader(id *string, buf int, filter ReaderFilter) (string, error)` - creates a reader and returns its ID;
- `DelReader(id string) error` - removes a reader;
- `AwaitWritersStop()` - waits for the journal to stop, then for writers to finish.

### Writer

The writer adds records to the journal. It is registered via `NewWriter` and removed via `DelWriter`.

API:
- `NewWriter() error` - registers a writer;
- `DelWriter() error` - removes a writer.

## Read/Write

> **Warning**
>
> A good reader must keep up with writers and handle bursts of unprocessed records.

### Read

Reading is performed by the `Read` and `ReadGroup` methods. Both methods find a reader by ID and do not block: if there is no data, they return an empty result immediately. `Read` returns one record, `ReadGroup` returns up to `MaxInGroup` records at a time. If no reader with such ID is found, both methods return an error.

API:
- `Read(id string) (*Record, error)` - returns one record, or `nil` if there is no data;
- `ReadGroup(id string) ([]*Record, error)` - returns up to `MaxInGroup` records; an empty slice if there is no data.

### Write

Writing is performed by the `Write` method. The method delivers the record to all readers whose filter allowed it. If `AlarmWrite` is set, delivery uses a timeout; readers that did not accept the record in time are considered blocked. If there are no readers or some of them are blocked, the record is lost, and an error message is written to `crashReport.kj`.

API:
- `Write(record *Record)` - delivers the record to matching readers.

## Metrics

`GetMetrics` returns a snapshot of the current journal state: status, number of writers and readers, and the fill level of each reader's channel.

| Name | Type | Description |
|---|---|---|
| `Status` | `string` | Journal status: `OFF`, `Running`, `Stop`, `ON`, or `Unknown` |
| `Writers` | `int` | Current number of writers |
| `Readers` | `int` | Current number of readers |
| `ReadersFill` | `map[string]struct{ Len, Cap int }` | For each reader: how many records are in the channel (`Len`) and the buffer size (`Cap`) |

## File Dump

If `FileDumperDir` is set, the journal saves records to files. The dump is performed by the dumper - a separate reader that takes records and writes them to disk. If `FileDumperDir` is empty, the dumper does not start.

Before work, the dumper prepares the folder: creates it if needed, checks that it is a directory, and checks that it is writable. A relative path is resolved from the executable's directory. After successful preparation, the path is stored as absolute.

Each record is written to a separate file named by time. The record is appended line by line in text form. Errors on open, write, and close go to `crashReport.kj`.

The dumper runs while the journal is running and finishes on shutdown. Before finishing, it flushes the accumulated records to a file.

## Critical Errors

If the journal works incorrectly or records do not arrive, look for the cause in `crashReport.kj`. The file is created next to the executable.

Everything that broke the journal is written there:
- a record arrived but there are no readers;
- a reader did not accept the record in time;
- writers or readers did not finish within the allotted time;
- file system errors while dumping records.

## Example

- **Lifecycle**
```go
journal := KJournal.NewKJournal()
settings := KJournal.KJSettings{
	DefaultReaderBuf:    512,                  // default reader buffer size
	DefaultReaderFilter: nil,                  // default filter (nil = pass all)
	MaxInGroup:          64,                   // max records per group read
	FileDumperDir:       "./records",          // folder for saving records (empty = dumper disabled)
	AlarmRWGroup:        5 * time.Second,      // group wait timeout (0 = no timeout)
	AlarmWrite:          2 * time.Second,      // reader channel write timeout (0 = no timeout)
}

// Startup
if err := journal.Run(settings); err != nil {
	panic(err)
}

// ... Journal work

// Shutdown
if err = journal.Endwork(); err != nil {
	fmt.Println(err.Error())
}
```

- **Getting a journal snapshot (metrics)**
```go
metrics := journal.GetMetrics()
```

- **Reader/Read**
```go
readerID, err := journal.NewReader(nil, 0, nil)
if err != nil {
	panic(err)
}
defer journal.DelReader(readerID)

// Auto-cancel: exit the loop once writers stop
done := make(chan struct{})
go func() {
	journal.AwaitWritersStop()
	close(done)
}()

for {
	select {
	case <-done:
		// read the remaining records and exit
		records, _ := journal.ReadGroup(readerID)
		for _, record := range records {
			fmt.Println(record.ToString())
		}
		return
	default:
		records, err := journal.ReadGroup(readerID)
		if err != nil {
			return
		}
		for _, record := range records {
			fmt.Println(record.ToString())
		}
	}
}
```

- **Writer/Write**
```go
// Register a writer
if err := journal.NewWriter(); err != nil {
	panic(err)
}
defer journal.DelWriter()

// Create and publish a record
record := KJournal.NewRecord("app", "Error", "E001", "something went wrong")
journal.Write(record)
```

---

This documentation is also available in [Russian](src/docs/README.ru.md).