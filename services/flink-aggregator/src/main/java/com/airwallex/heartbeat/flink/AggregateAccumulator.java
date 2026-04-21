package com.airwallex.heartbeat.flink;

import com.tdunning.math.stats.MergingDigest;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Accumulator for a single (region, status, window) key.
 *
 * <p>We keep:
 * <ul>
 *   <li>A {@link MergingDigest} for p50/p95/p99 with bounded memory
 *       (~ a few KiB per key).</li>
 *   <li>A running sum + count to derive the exact mean.</li>
 *   <li>A plain HashSet of device ids for distinct count. This is fine
 *       up to a few 100k distinct devices per window; for higher cardinality
 *       swap for HLL (see DistinctCountAccumulator).</li>
 * </ul>
 *
 * <p>Kept package-private so we can unit-test without reflection tricks.
 */
class AggregateAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;
    // 100 compression is Flink's community default for t-digest; good
    // accuracy/size tradeoff.
    private static final double COMPRESSION = 100;

    MergingDigest digest = new MergingDigest(COMPRESSION);
    double latencySum;
    long   count;
    Set<String> devices = new HashSet<>();

    void add(Heartbeat h) {
        digest.add(h.latencyMs);
        latencySum += h.latencyMs;
        count++;
        devices.add(h.deviceId);
    }

    AggregateAccumulator merge(AggregateAccumulator other) {
        // MergingDigest.add(MergingDigest) preserves bounded memory even
        // during session/global window merges.
        this.digest.add(other.digest);
        this.latencySum += other.latencySum;
        this.count      += other.count;
        this.devices.addAll(other.devices);
        return this;
    }

    double avg() { return count == 0 ? 0 : latencySum / count; }
    double p(double q) { return count == 0 ? 0 : digest.quantile(q); }
}
