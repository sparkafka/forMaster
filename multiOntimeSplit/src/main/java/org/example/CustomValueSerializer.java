package org.example;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CustomValueSerializer implements Serializer<CustomValue> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String s, CustomValue customValue) {
        return new byte[0];
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
