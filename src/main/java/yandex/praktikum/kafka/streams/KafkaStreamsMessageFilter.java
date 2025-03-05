package yandex.praktikum.kafka.streams;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Component;
import yandex.praktikum.kafka.config.KafkaProperties;
import yandex.praktikum.kafka.config.KafkaStreamProperties;
import yandex.praktikum.kafka.dto.BlockedUsers;
import yandex.praktikum.kafka.dto.MyMessage;
import yandex.praktikum.kafka.dto.MyMessageSerdes;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaStreamsMessageFilter {

    private final KafkaProperties kafkaProperties;
    private final KafkaStreamProperties kafkaStreamProperties;
    private final KafkaDeprecatedWordsHandler kafkaDeprecatedWordsHandler;
    private final KafkaBlockedUsersHandler kafkaBlockedUsersHandler;

    @PostConstruct
    public void init() {
        ExecutorService kafkaStreamsExecutorService = Executors.newSingleThreadExecutor();
        kafkaStreamsExecutorService.submit(this::process);
    }

    private void process() {
        try {
            // Конфигурация Kafka Streams
            Properties properties = kafkaStreamProperties.getProperties("praktikum-streams-app");

            // Создание StreamsBuilder
            StreamsBuilder builder = new StreamsBuilder();

            KStream<Integer, MyMessage> messagesStream = builder.stream(kafkaProperties.getTopicMessages(),
                    Consumed.with(Serdes.Integer(), new MyMessageSerdes()));

            // Фильтрация пользователей
            KStream<Integer, MyMessage> filteredUsers = messagesStream.filter(((key, myMessage) -> filterUsers(myMessage)));

            // Отправка отфильтрованных данных в другой топик
            filteredUsers.mapValues(this::filterWords)
                    .to(kafkaProperties.getTopicFilteredMessages());

            // Инициализация Kafka Streams
            KafkaStreams streams = new KafkaStreams(builder.build(), properties);

            // Устанавливаем обработчик необработанных исключений, чтобы при ошибках поток останавливался корректно.
            streams.setUncaughtExceptionHandler(exception -> {
                log.error("Ошибка при работе praktikum-streams-app", exception);
                return SHUTDOWN_CLIENT;
            });

            // Запуск приложения
            streams.start();
            log.info("Kafka Streams приложение запущено успешно!");
        } catch (Exception ex) {
            log.error("Ошибка при запуске Kafka Streams приложения: ", ex);
        }
    }

    private boolean filterUsers(MyMessage myMessage) {
        List<String> blockedUsersForUser = kafkaBlockedUsersHandler.getBlockedUsersForUser(myMessage.getRecipient());
        return !blockedUsersForUser.contains(myMessage.getAuthor());
    }

    private MyMessage filterWords(MyMessage myMessage) {
        KeyValueIterator<String, String> keyValueIterator = kafkaDeprecatedWordsHandler.getDeprecatedWordsIterator();
        while (keyValueIterator.hasNext()) {
            String currDeprWord = keyValueIterator.next().value;
            String message = myMessage.getMessage();
            if (message.contains(currDeprWord)) {
                String newMessage = message.replace(currDeprWord, "*");
                myMessage.setMessage(newMessage);
            }
        }
        return myMessage;
    }
}
