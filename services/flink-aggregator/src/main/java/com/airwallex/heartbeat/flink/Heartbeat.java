package com.airwallex.heartbeat.flink;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Mirror of the Go wire type. Kept as a plain POJO so Flink can use its
 * POJO serializer (faster + schema evolution friendly vs. generic Kryo).
 */
public class Heartbeat implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("device_id")   public String  deviceId;
    @JsonProperty("region")      public String  region;
    @JsonProperty("status")      public String  status;
    @JsonProperty("timestamp_ms") public long   timestampMs;
    @JsonProperty("latency_ms")  public double  latencyMs;
    @JsonProperty("cpu_usage")   public double  cpuUsage;
    @JsonProperty("mem_usage")   public double  memUsage;

    public Heartbeat() {}
}
