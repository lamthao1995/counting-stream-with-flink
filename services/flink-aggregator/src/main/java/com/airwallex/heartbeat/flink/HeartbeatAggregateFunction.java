package com.airwallex.heartbeat.flink;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Flink {@link AggregateFunction} that accumulates latency stats and
 * distinct device ids for a single window. The output is an
 * {@link AggregateAccumulator}; a follow-up {@code ProcessWindowFunction}
 * decorates it with window metadata and the (region, status) key.
 */
public class HeartbeatAggregateFunction
        implements AggregateFunction<Heartbeat, AggregateAccumulator, AggregateAccumulator> {

    @Override public AggregateAccumulator createAccumulator() { return new AggregateAccumulator(); }
    @Override public AggregateAccumulator add(Heartbeat h, AggregateAccumulator acc) { acc.add(h); return acc; }
    @Override public AggregateAccumulator getResult(AggregateAccumulator acc) { return acc; }
    @Override public AggregateAccumulator merge(AggregateAccumulator a, AggregateAccumulator b) { return a.merge(b); }
}
