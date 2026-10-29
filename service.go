package KJournal

import (
	"fmt"
)

//region Metrics

// desc: channel usage snapshot (len/cap)
type ChanMetrics struct {
	Len int
	Cap int
}

// desc: full journal state snapshot
type KJournalMetrics struct {
	Status             Status
	WritersCount       int
	ReadersCount       int
	ReadersChanMetrics map[string]ChanMetrics
}

// desc: collects a journal metrics snapshot
func (kj *KJournal) GetMetrics() KJournalMetrics {
	m := KJournalMetrics{
		Status:             kj.status,
		WritersCount:       int(kj.writersCount.Load()),
		ReadersCount:       int(kj.readersCount.Load()),
		ReadersChanMetrics: make(map[string]ChanMetrics),
	}

	kj.readers.Range(func(k, v any) bool {
		ch := v.(Reader)
		m.ReadersChanMetrics[k.(string)] = ChanMetrics{
			Len: len(ch.Chan),
			Cap: cap(ch.Chan),
		}
		return true
	})

	return m
}

//region^ Formatting

// desc: renders metrics as human-readable text
func (kjm *KJournalMetrics) ToFormattedText() string {
	text := "Journal\n"
	text += fmt.Sprintf("    Status: %s\n    Writers: %d\n    Readers: %d\n", kjm.Status, kjm.WritersCount, kjm.ReadersCount)
	text += "\tReaders\n"
	for id, metrics := range kjm.ReadersChanMetrics {
		var idclear string
		if len(id) >= 8 {
			idclear = id[:8] + "..."
		} else {
			idclear = id
		}
		text += fmt.Sprintf("        %s: %d/%d\n", idclear, metrics.Len, metrics.Cap)
	}

	return text
}

//endregion^

//endregion
