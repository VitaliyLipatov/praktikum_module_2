package yandex.praktikum.kafka.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class MyMessageDeserializer implements Deserializer<MyMessage> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public MyMessage deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return objectMapper.readValue(bytes, MyMessage.class);
        } catch (Exception ex) {
            throw new RuntimeException("Ошибка десериализации объекта MyMessage", ex);
        }
    }
}
