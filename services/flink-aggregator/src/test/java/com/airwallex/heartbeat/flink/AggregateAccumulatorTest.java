package com.airwallex.heartbeat.flink;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class AggregateAccumulatorTest {

    private static Heartbeat hb(String id, double lat) {
        Heartbeat h = new Heartbeat();
        h.deviceId = id;
        h.region = "us-east";
        h.status = "ok";
        h.timestampMs = 1;
        h.latencyMs = lat;
        return h;
    }

    @Test
    void emptyAccumulatorReturnsZeros() {
        AggregateAccumulator a = new AggregateAccumulator();
        assertEquals(0, a.count);
        assertEquals(0.0, a.avg());
        assertEquals(0.0, a.p(0.99));
    }

    @Test
    void addComputesExactMean() {
        AggregateAccumulator a = new AggregateAccumulator();
        a.add(hb("d1", 10.0));
        a.add(hb("d1", 20.0));
        a.add(hb("d2", 30.0));
        assertEquals(3, a.count);
        assertEquals(20.0, a.avg(), 1e-9);
        assertEquals(2, a.devices.size());
    }

    @Test
    void percentilesAreApproximatelyCorrect() {
        AggregateAccumulator a = new AggregateAccumulator();
        // Uniform 1..1000: p95 ~= 950, p99 ~= 990. t-digest should be
        // within a percent on this clean distribution.
        for (int i = 1; i <= 1000; i++) a.add(hb("d" + i, i));
        assertEquals(500, a.p(0.50), 5);
        assertEquals(950, a.p(0.95), 10);
        assertEquals(990, a.p(0.99), 10);
        assertEquals(1000, a.devices.size());
    }

    @Test
    void mergeIsAssociative() {
        AggregateAccumulator a = new AggregateAccumulator();
        AggregateAccumulator b = new AggregateAccumulator();
        Random r = new Random(42);
        for (int i = 0; i < 500; i++) a.add(hb("dA" + i, r.nextDouble() * 100));
        for (int i = 0; i < 500; i++) b.add(hb("dB" + i, r.nextDouble() * 100));

        AggregateAccumulator merged = new AggregateAccumulator().merge(a).merge(b);
        assertEquals(1000, merged.count);
        assertEquals(1000, merged.devices.size());
        // mean must be exact after merge
        double expected = (a.latencySum + b.latencySum) / 1000.0;
        assertEquals(expected, merged.avg(), 1e-9);
    }

    @Test
    void distinctSetCountsUnique() {
        AggregateAccumulator a = new AggregateAccumulator();
        a.add(hb("d1", 1));
        a.add(hb("d1", 2));
        a.add(hb("d1", 3));
        a.add(hb("d2", 4));
        assertEquals(4, a.count);
        assertEquals(2, a.devices.size());
    }
}
