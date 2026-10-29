# KJournal

## Description
KJournal is an in-memory journal for Go. It works in real time. It supports reader and writer registration. Each reader has its own channel and its own rules to filter records.

## Lifecycle
The journal has four states: OFF, RUNNING, ON, STOP. Run() moves the journal from OFF to ON if it is not running yet. EndWork() starts the shutdown: the journal goes to STOP, waits for readers and writers, then returns to OFF. Repeated calls to Run() and EndWork() do nothing.

> **Warning**
>
> `KJournal` lives only in memory. It lives exactly as long as your program runs. When you restart the program, all records and registered readers are lost — you cannot save the journal between runs.

## Entities

### Record
Record is a single unit of data in the journal. It has five fields: `Author`, `Level`, `Code`, `Time`, and `Message`.
You can create a record using `NewRecord(author, level, code, message)` — the time will be added automatically. To display it in a readable format, use `ToString()`.

### Writer
A Writer is an entity that writes records to the journal. To become a writer, call `AddWriter()`, and when you are done, call `DelWriter()`.

> **Warning**
>
> While a writer is active, the journal will wait for it during shutdown (`EndWork`). If you do not call `DelWriter()`, the shutdown will hang.

### Reader
Reader is an entity that receives records from the journal. Each reader has its own buffered channel and its own filtering rules (`Rules`): a rule decides which records the reader needs.

Registration — `AddReader(id, buff, rules)`. The `id` can be `nil` (a UUID will be generated), `buff` sets the channel buffer size, and `rules` is the filter (if `nil`, the reader gets all records). The method returns the reader's ID.

Removal — `DelReader(id)`.

> **Warning 1**
>
> While the reader is registered, the journal will wait for it during shutdown (`EndWork`). If you do not call `DelReader()`, the shutdown will hang.

> **Warning 2**
>
> The reader must be registered before the writer, otherwise some records will be lost.

## Working with records

### Writing
Writing to the journal is done via `Write(record)`. The journal sends the record to every reader whose filter (`Rules`) returned `true`. Delivery goes to the reader's buffered channel.

> **Warning**
>
> `Write` is blocking: if the channel of at least one matching reader is full, `Write` will wait until space frees up. Other readers will not get this record during that time.

### Reading
Reading is done via `Read(id)`: this is a non-blocking read from the reader's channel. If a record exists, it is returned with `true`. If the channel is empty or the reader is not found, it returns `(nil, false)`.

## Metrics
The journal can provide a snapshot of its state via `GetMetrics()`. In the snapshot (`KJournalMetrics`):

- `Status` — the current status of the journal;
- `WritersCount` — the number of active writers;
- `ReadersCount` — the number of registered readers;
- `ReadersChanMetrics` — the fill level of each reader's channel: `Len` (how many records are in the channel) and `Cap` (channel capacity).

To display it in a readable format, use `ToFormattedText()` — a text report with this data.

## Example

```go
func main() {
	// create the journal and start it
	j := &KJournal.KJournal{}
	j.Run()

	// register the reader before the writer,
	// otherwise some records will be lost
	id := j.AddReader(nil, 10, nil)

	// register the writer
	j.AddWriter()

	// write a record to the journal
	j.Write(KJournal.NewRecord("app", "INFO", "200", "hello"))

	// read a record from the reader's channel
	record, ok := j.Read(id)
	if ok {
		fmt.Println(record.ToString())
	}

	// get the metrics
	metrics := j.GetMetrics()
	fmt.Println(metrics.ToFormattedText())

	// shut down: first the writer, then the reader,
	// stop the journal last
	j.DelWriter()
	j.DelReader(id)
	j.EndWork()
}
```