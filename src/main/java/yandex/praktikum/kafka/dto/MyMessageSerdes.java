package yandex.praktikum.kafka.dto;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class MyMessageSerdes extends Serdes.WrapperSerde<MyMessage> {

    public MyMessageSerdes() {
        super(new MyMessageSerializer(), new MyMessageDeserializer());
    }
}
