package kafkax

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/airwallex/heartbeat/pkg/model"
)

func TestFakeProducer_Publish(t *testing.T) {
	f := &FakeProducer{}
	evs := []model.Heartbeat{{DeviceID: "a"}, {DeviceID: "b"}}
	if err := f.Publish(context.Background(), evs); err != nil {
		t.Fatal(err)
	}
	if got := f.Snapshot(); len(got) != 2 || got[0].DeviceID != "a" {
		t.Fatalf("unexpected snapshot: %+v", got)
	}
	if f.CallCount != 1 {
		t.Fatalf("call count = %d, want 1", f.CallCount)
	}
}

func TestFakeProducer_Error(t *testing.T) {
	want := errors.New("boom")
	f := &FakeProducer{Err: want}
	if err := f.Publish(context.Background(), []model.Heartbeat{{DeviceID: "a"}}); !errors.Is(err, want) {
		t.Fatalf("want err %v, got %v", want, err)
	}
	if n := len(f.Snapshot()); n != 0 {
		t.Fatalf("want nothing published on error, got %d", n)
	}
}

func TestFakeProducer_Concurrent(t *testing.T) {
	f := &FakeProducer{}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = f.Publish(context.Background(), []model.Heartbeat{{DeviceID: "d"}})
		}(i)
	}
	wg.Wait()
	if got := len(f.Snapshot()); got != 50 {
		t.Fatalf("want 50 events, got %d", got)
	}
}
