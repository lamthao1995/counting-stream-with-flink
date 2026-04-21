package com.airwallex.heartbeat.flink;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * JSON -> Heartbeat deserializer. Malformed events are dropped (returning
 * null tells Flink to skip); a real deployment would also emit them to a
 * DLQ side output.
 */
public class HeartbeatDeserializer implements DeserializationSchema<Heartbeat> {
    private static final long serialVersionUID = 1L;
    private transient ObjectMapper mapper;

    @Override
    public Heartbeat deserialize(byte[] message) {
        if (mapper == null) {
            mapper = new ObjectMapper().configure(
                    DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
        try {
            return mapper.readValue(message, Heartbeat.class);
        } catch (Exception e) {
            return null;
        }
    }

    @Override public boolean isEndOfStream(Heartbeat nextElement) { return false; }
    @Override public TypeInformation<Heartbeat> getProducedType() {
        return TypeInformation.of(Heartbeat.class);
    }
}
