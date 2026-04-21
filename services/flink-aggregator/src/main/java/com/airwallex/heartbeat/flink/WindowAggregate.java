package com.airwallex.heartbeat.flink;

import java.io.Serializable;

/** Output row written to ClickHouse. */
public class WindowAggregate implements Serializable {
    private static final long serialVersionUID = 1L;

    public long   windowStartMs;
    public long   windowEndMs;
    public String region;
    public String status;
    public long   count;
    public long   distinctCount;
    public double avgLatencyMs;
    public double p50LatencyMs;
    public double p95LatencyMs;
    public double p99LatencyMs;

    public WindowAggregate() {}
}
