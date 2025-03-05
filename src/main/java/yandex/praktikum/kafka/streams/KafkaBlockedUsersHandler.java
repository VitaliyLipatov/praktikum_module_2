package yandex.praktikum.kafka.streams;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import yandex.praktikum.kafka.config.KafkaProperties;
import yandex.praktikum.kafka.config.KafkaStreamProperties;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;

@Slf4j
@Component
public class KafkaBlockedUsersHandler {

    private final KafkaProperties kafkaProperties;
    private final KafkaStreamProperties kafkaStreamProperties;
    private final ReadOnlyKeyValueStore<String, String> readOnlyKeyValueStore;

    @Autowired
    public KafkaBlockedUsersHandler(KafkaProperties kafkaProperties, KafkaStreamProperties kafkaStreamProperties) {
        this.kafkaProperties = kafkaProperties;
        this.kafkaStreamProperties = kafkaStreamProperties;
        this.readOnlyKeyValueStore = setReadOnlyKeyValueStore();
    }

    public ReadOnlyKeyValueStore<String, String> setReadOnlyKeyValueStore() {
        try {
            // Конфигурация Kafka Streams
            Properties config = kafkaStreamProperties.getProperties("blocked-users-to-table");
            config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

            // Создание топологии
            StreamsBuilder builder = new StreamsBuilder();
            // Создание потока из Kafka-топика
            KStream<String, String> stream = builder.stream(kafkaProperties.getTopicBlockedUsers(),
                    Consumed.with(Serdes.String(), Serdes.String()));
            // Преобразование потока в таблицу с помощью метода toTable()
            stream.toTable(Materialized.<String, String>as(Stores.inMemoryKeyValueStore("blocked-users-store"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String()));

            // Инициализация Kafka Streams
            KafkaStreams streams = new KafkaStreams(builder.build(), config);

            // Устанавливаем обработчик необработанных исключений, чтобы при ошибках поток останавливался корректно.
            streams.setUncaughtExceptionHandler(exception -> {
                log.error("Ошибка при работе blocked-users-to-table", exception);
                return SHUTDOWN_CLIENT;
            });
            // Очищаем состояние Kafka Streams перед запуском
            streams.cleanUp();
            streams.start();
            log.info("Kafka Streams приложение запущено успешно.");


            while (streams.state() != KafkaStreams.State.RUNNING) {
                Thread.sleep(5000); // Ждём инициализации состояния
                log.info("Текущее состояние stream blocked-users-to-table : {}", streams.state());
                if (streams.state() == KafkaStreams.State.ERROR || streams.state() == KafkaStreams.State.NOT_RUNNING) {
                    throw new RuntimeException("Stream is not started");
                }
            }

            // Запрашиваем хранилище с именем blocked-users-to-table для извлечения данных.
            return streams.store(
                    fromNameAndType("blocked-users-store", QueryableStoreTypes.keyValueStore())
            );

        } catch (Exception ex) {
            log.error("Ошибка при работе Kafka Streams приложения: ", ex);
            throw new RuntimeException(ex);
        }
    }

    public List<String> getBlockedUsersForUser(final String user) {
        String blockedUsers = readOnlyKeyValueStore.get(user);
        if (blockedUsers != null) {
            return Arrays.stream(blockedUsers.split(";")).toList();
        } else {
            return List.of();
        }
    }
}
