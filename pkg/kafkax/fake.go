package kafkax

import (
	"context"
	"sync"

	"github.com/airwallex/heartbeat/pkg/model"
)

// FakeProducer captures published events in memory. It is safe for
// concurrent use - handlers under test may publish from many goroutines.
type FakeProducer struct {
	mu        sync.Mutex
	Published []model.Heartbeat
	Err       error // if set, Publish returns this error
	CallCount int
}

func (f *FakeProducer) Publish(_ context.Context, events []model.Heartbeat) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.CallCount++
	if f.Err != nil {
		return f.Err
	}
	f.Published = append(f.Published, events...)
	return nil
}

func (f *FakeProducer) Close() error { return nil }

// Snapshot returns a copy of the published events. Useful to assert
// without holding the lock across the test.
func (f *FakeProducer) Snapshot() []model.Heartbeat {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]model.Heartbeat, len(f.Published))
	copy(out, f.Published)
	return out
}
