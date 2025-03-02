package yandex.praktikum.kafka.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.stereotype.Component;
import yandex.praktikum.kafka.dto.MyMessageSerdes;

import java.util.Properties;

@Component
@RequiredArgsConstructor
public class KafkaStreamProperties {

    private final KafkaProperties kafkaProperties;

    public Properties getProperties(String appName) {
        Properties properties = new Properties();

        // Уникальный идентификатор приложения Kafka Streams.
        // Помогает координировать и отслеживать состояния приложения внутри Kafka.
        // Все экземпляры приложения с одинаковым ID считаются частью одной группы.
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);

        // Список адресов Kafka-брокеров, к которым будет подключаться приложение.
        // Здесь указывается минимум один брокер для начальной связи с кластером Kafka.
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());

        // Указываем класс для сериализации и десериализации ключей сообщений по умолчанию.
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());

        // Указываем класс для сериализации и десериализации значений сообщений по умолчанию.
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MyMessageSerdes.class.getName());
        return properties;
    }
}
