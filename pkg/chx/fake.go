package chx

import (
	"context"
	"sort"
	"strings"

	"github.com/airwallex/heartbeat/pkg/model"
)

// FakeRunner is an in-memory QueryRunner for tests.
type FakeRunner struct {
	Aggregates []model.WindowAggregate
	Distinct   uint64
	Err        error

	// LastFilter records what the handler actually asked for.
	LastFilter Filter
}

func (f *FakeRunner) FetchAggregates(_ context.Context, flt Filter) ([]model.WindowAggregate, error) {
	f.LastFilter = flt
	if f.Err != nil {
		return nil, f.Err
	}
	var out []model.WindowAggregate
	for _, a := range f.Aggregates {
		if a.WindowStartMs < flt.FromMs || a.WindowStartMs >= flt.ToMs {
			continue
		}
		if flt.Region != "" && !strings.EqualFold(a.Region, flt.Region) {
			continue
		}
		if flt.Status != "" && !strings.EqualFold(a.Status, flt.Status) {
			continue
		}
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WindowStartMs < out[j].WindowStartMs })
	if flt.Limit > 0 && len(out) > flt.Limit {
		out = out[:flt.Limit]
	}
	return out, nil
}

func (f *FakeRunner) DistinctDevices(_ context.Context, flt Filter) (uint64, error) {
	f.LastFilter = flt
	if f.Err != nil {
		return 0, f.Err
	}
	return f.Distinct, nil
}

func (f *FakeRunner) Ping(context.Context) error { return f.Err }
func (f *FakeRunner) Close() error               { return nil }
