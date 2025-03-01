package yandex.praktikum.kafka.streams;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import yandex.praktikum.kafka.config.KafkaStreamProperties;
import yandex.praktikum.kafka.dto.MyMessage;
import yandex.praktikum.kafka.dto.MyMessageSerdes;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaStreamsMessageFilter {

    private final KafkaStreamProperties kafkaStreamProperties;

    @EventListener(ApplicationStartedEvent.class)
    public void init() {
        ExecutorService kafkaStreamsExecutorService = Executors.newSingleThreadExecutor();
		kafkaStreamsExecutorService.submit(this::process);
    }

    private void process() {
        try {
            // Конфигурация Kafka Streams
            Properties properties = kafkaStreamProperties.getProperties();

            // Создание StreamsBuilder
            StreamsBuilder builder = new StreamsBuilder();

            KStream<Integer, MyMessage> messagesStream = builder.stream("messages",
                    Consumed.with(Serdes.Integer(), new MyMessageSerdes()));

            // Фильтрация пользователей
            KStream<Integer, String> processedStream = messagesStream.mapValues(
                    myMessage -> "Processed: " + myMessage.getMessage());

            // Отправка обработанных данных в другой топик
            processedStream.to("output-topic");

            // Инициализация Kafka Streams
            KafkaStreams streams = new KafkaStreams(builder.build(), properties);

            // Запуск приложения
            streams.start();
            log.info("Kafka Streams приложение запущено успешно!");
        } catch (Exception ex) {
            log.error("Ошибка при запуске Kafka Streams приложения: ", ex);
        }
    }
}
