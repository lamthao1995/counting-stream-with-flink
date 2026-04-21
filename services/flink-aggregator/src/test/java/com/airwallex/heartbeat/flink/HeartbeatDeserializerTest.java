package com.airwallex.heartbeat.flink;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class HeartbeatDeserializerTest {

    @Test
    void deserializeGoodJson() {
        HeartbeatDeserializer d = new HeartbeatDeserializer();
        byte[] b = ("{\"device_id\":\"dev-1\",\"region\":\"us\",\"status\":\"ok\"," +
                    "\"timestamp_ms\":123,\"latency_ms\":42.5," +
                    "\"some_unknown_field\":true}").getBytes(StandardCharsets.UTF_8);
        Heartbeat h = d.deserialize(b);
        assertNotNull(h);
        assertEquals("dev-1", h.deviceId);
        assertEquals(42.5, h.latencyMs, 1e-9);
    }

    @Test
    void malformedReturnsNull() {
        HeartbeatDeserializer d = new HeartbeatDeserializer();
        assertNull(d.deserialize("not-json".getBytes(StandardCharsets.UTF_8)));
    }
}
