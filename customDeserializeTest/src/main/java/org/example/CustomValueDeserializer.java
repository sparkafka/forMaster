package org.example;

import com.sun.xml.internal.ws.encoding.soap.DeserializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomValueDeserializer implements Deserializer<CustomValue> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public CustomValue deserialize(String s, byte[] customValue) {
        try{
            if (customValue == null){
                System.out.println("Null");
                return null;
            }
            // wrap: byte []를 ByteBuffer로 만듭니다.
            ByteBuffer buf = ByteBuffer.wrap(customValue);
            int window = buf.getInt();
            int value = buf.getInt();
            Long eventTime = buf.getLong();
            Long endTime = buf.getLong();

            return new CustomValue(window, value, eventTime, endTime);
        } catch (Exception e){
            throw new DeserializationException("Deserialization Error");
        }
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
