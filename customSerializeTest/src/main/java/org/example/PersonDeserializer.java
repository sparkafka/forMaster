package org.example;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class PersonDeserializer implements Deserializer<Person> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public Person deserialize(String s, byte[] person) {
        try{
            if (person == null){
                System.out.println("Null");
                return null;
            }
            ByteBuffer buf = ByteBuffer.wrap(person);
            int sizeOfName = buf.getInt();
            byte[] nameBytes = new byte[sizeOfName];
            buf.get(nameBytes); // nameBytes에 저장됨
            String deserializedName = new String(nameBytes, StandardCharsets.UTF_8);
            int age = buf.getInt();

            return new Person(deserializedName, age);
        } catch (Exception e){
            throw new SerializationException("Error when deserializing byte[] to Person");
        }
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
