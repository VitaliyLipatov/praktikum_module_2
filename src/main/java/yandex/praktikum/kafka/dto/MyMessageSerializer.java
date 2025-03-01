package yandex.praktikum.kafka.dto;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class MyMessageSerializer implements Serializer<MyMessage> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, MyMessage myMessage) {
        if (myMessage == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(myMessage);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException("Ошибка сериализации объекта MyMessage", ex);
        }
    }
}
