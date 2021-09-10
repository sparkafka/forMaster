package org.example;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class PersonSerializer implements Serializer<Person> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String s, Person person) {
        int sizeOfName;
        byte[] serializedName;

        try {
            if (person == null) return null;

            serializedName = person.getName().getBytes(StandardCharsets.UTF_8);
            sizeOfName = serializedName.length;

            // ByteBuffer 는 바이트 데이터를 저장하고 읽는 저장소
            // 네트워크 통신 등에 사용
            // ByteBuffer에 전달할 Byte 크기 할당
            ByteBuffer buf = ByteBuffer.allocate(sizeOfName + 4 + 4); // 문자열 + 문자열 크기(int) + int
            buf.putInt(sizeOfName);
            buf.put(serializedName);
            buf.putInt(person.getAge());

            return buf.array();
        } catch (Exception e){
            throw new SerializationException("Error when serializing Person to byte[]");
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Person data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
