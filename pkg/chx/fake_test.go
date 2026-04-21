package chx

import (
	"context"
	"testing"

	"github.com/airwallex/heartbeat/pkg/model"
)

func TestFakeRunner_FetchAggregates_Filters(t *testing.T) {
	r := &FakeRunner{
		Aggregates: []model.WindowAggregate{
			{WindowStartMs: 100, Region: "us", Status: "ok"},
			{WindowStartMs: 200, Region: "eu", Status: "ok"},
			{WindowStartMs: 300, Region: "us", Status: "warn"},
			{WindowStartMs: 400, Region: "us", Status: "ok"},
		},
	}
	got, err := r.FetchAggregates(context.Background(), Filter{FromMs: 100, ToMs: 400, Region: "us"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2, got %d", len(got))
	}
	if got[0].WindowStartMs != 100 || got[1].WindowStartMs != 300 {
		t.Fatalf("unexpected order: %+v", got)
	}
	if r.LastFilter.Region != "us" {
		t.Fatalf("LastFilter not recorded")
	}
}

func TestFakeRunner_FetchAggregates_Limit(t *testing.T) {
	r := &FakeRunner{
		Aggregates: []model.WindowAggregate{
			{WindowStartMs: 1}, {WindowStartMs: 2}, {WindowStartMs: 3},
		},
	}
	got, _ := r.FetchAggregates(context.Background(), Filter{FromMs: 0, ToMs: 10, Limit: 2})
	if len(got) != 2 {
		t.Fatalf("want 2, got %d", len(got))
	}
}
