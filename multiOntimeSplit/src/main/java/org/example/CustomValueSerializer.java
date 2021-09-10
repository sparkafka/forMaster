package org.example;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomValueSerializer implements Serializer<CustomValue> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String s, CustomValue customValue) {
        try {
            if (customValue == null) return null;

            ByteBuffer buf = ByteBuffer.allocate(4 + 4 + 4 + 8 + 8);
            buf.putInt(customValue.window);
            buf.putInt(customValue.value);
            buf.putInt(customValue.nodeNum);
            buf.putLong(customValue.eventTime);
            buf.putLong(customValue.endTime);

            return buf.array();
        } catch (Exception e) {
            throw new SerializationException("Serialization Error");
        }
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
