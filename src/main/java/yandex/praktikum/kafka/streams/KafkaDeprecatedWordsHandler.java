package yandex.praktikum.kafka.streams;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import yandex.praktikum.kafka.config.KafkaProperties;
import yandex.praktikum.kafka.config.KafkaStreamProperties;

import java.util.Properties;

import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;

@Slf4j
@Getter
@Component
@RequiredArgsConstructor
public class KafkaDeprecatedWordsHandler {

    private final KafkaProperties kafkaProperties;
    private final KafkaStreamProperties kafkaStreamProperties;
    private final ReadOnlyKeyValueStore<String, String> readOnlyKeyValueStore;

    @Autowired
    public KafkaDeprecatedWordsHandler(KafkaProperties kafkaProperties, KafkaStreamProperties kafkaStreamProperties) {
        this.kafkaProperties = kafkaProperties;
        this.kafkaStreamProperties = kafkaStreamProperties;
        this.readOnlyKeyValueStore = setReadOnlyKeyValueStore();
    }

    private ReadOnlyKeyValueStore<String, String> setReadOnlyKeyValueStore() {
        try {
            // Конфигурация Kafka Streams
            Properties config = kafkaStreamProperties.getProperties("deprecated-words-to-table");
            config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

            // Создание топологии
            StreamsBuilder builder = new StreamsBuilder();
            // Создание потока из Kafka-топика
            KStream<String, String> stream = builder.stream(kafkaProperties.getTopicDeprecatedWords(),
                    Consumed.with(Serdes.String(), Serdes.String()));
            // Преобразование потока в таблицу с помощью метода toTable()
            stream.toTable(Materialized.<String, String>as(Stores.inMemoryKeyValueStore("deprecated-word-store"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String()));

            // Инициализация Kafka Streams
            KafkaStreams streams = new KafkaStreams(builder.build(), config);

            // Устанавливаем обработчик необработанных исключений, чтобы при ошибках поток останавливался корректно.
            streams.setUncaughtExceptionHandler(exception -> {
                log.error("Ошибка при работе deprecated-words-to-table", exception);
                return SHUTDOWN_CLIENT;
            });
            // Очищаем состояние Kafka Streams перед запуском
            streams.cleanUp();
            streams.start();
            log.info("Kafka Streams приложение запущено успешно.");


            while (streams.state() != KafkaStreams.State.RUNNING) {
                Thread.sleep(5000); // Ждём инициализации состояния
                log.info("Текущее состояние stream deprecated-words-to-table : {}", streams.state());
                if (streams.state() == KafkaStreams.State.ERROR || streams.state() == KafkaStreams.State.NOT_RUNNING) {
                    throw new RuntimeException("Stream is not started");
                }
            }

            // Запрашиваем хранилище с именем deprecated-word-store для извлечения данных.
            return streams.store(
                    fromNameAndType("deprecated-word-store", QueryableStoreTypes.keyValueStore())
            );
        } catch (Exception ex) {
            log.error("Ошибка при работе Kafka Streams приложения: ", ex);
            throw new RuntimeException(ex);
        }
    }

    public KeyValueIterator<String, String> getDeprecatedWordsIterator() {
        return readOnlyKeyValueStore.all();
    }
}
