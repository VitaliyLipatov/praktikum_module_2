package yandex.praktikum.kafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import yandex.praktikum.kafka.config.KafkaProperties;
import yandex.praktikum.kafka.dto.BlockedUsers;
import yandex.praktikum.kafka.dto.MyMessage;
import yandex.praktikum.kafka.dto.MyMessageSerializer;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class Producer {

    private static final List<String> INITIAL_DEPRECATED_WORDS = List.of("fuck", "shit");
    private static final List<String> RANDOM_WORDS = List.of("Hello", "World", "buddy", "what", "shit", "piece", "man",
            "of", "a", "fuck");
    private final KafkaProperties kafkaProperties;

    @Scheduled(fixedDelayString = "20", timeUnit = TimeUnit.SECONDS)
    public void sendAllMessagesInAllTopicsForExample() {
        KafkaProducer<Integer, MyMessage> myMessageProducer = getMyMessageProducer();

        // Отправка сообщений в топик
        int randomNumber = getRandomNumber(10);
        for (int i = 0; i < randomNumber; i++) {
            var myMessage = MyMessage.builder()
                    .author("user" + "-" + i)
                    .message(getMessage())
                    .recipient("user" + "-" + getRandomNumber(10))
                    .build();
            ProducerRecord<Integer, MyMessage> record = new ProducerRecord<>(kafkaProperties.getTopicMessages(), i,
                    myMessage);
            myMessageProducer.send(record);
            log.info("Сообщение {} успешно отправлено в топик {}", record.value(), kafkaProperties.getTopicMessages());
        }
        myMessageProducer.close();

        INITIAL_DEPRECATED_WORDS.forEach(this::sendDeprecatedWord);

        BlockedUsers firstBlockedUser = BlockedUsers.builder()
                .user("user" + "-" + getRandomNumber(10))
                .blockedUsers(List.of("user" + "-" + randomNumber, "user" + "-" + ++randomNumber))
                .build();

        BlockedUsers secondBlockedUser = BlockedUsers.builder()
                .user("user" + "-" + getRandomNumber(10))
                .blockedUsers(List.of("user" + "-" + randomNumber, "user" + "-" + ++randomNumber))
                .build();

        sendBlockedUsers(firstBlockedUser);
        sendBlockedUsers(secondBlockedUser);
    }

    public void sendBlockedUsers(BlockedUsers blockedUsers) {
        List<String> strings = blockedUsers.getBlockedUsers();
        String value = String.join(";", strings);
        ProducerRecord<String, String> record = new ProducerRecord<>(kafkaProperties.getTopicBlockedUsers(),
                blockedUsers.getUser(), value);
        KafkaProducer<String, String> stringProducer = getStringProducer();
        stringProducer.send(record);
        log.info("Для пользователя {} будут заблокированы сообщения от пользователей {}. "
                        + "Сообщение успешно отправлено в топик {}", blockedUsers.getUser(), record.value(),
                kafkaProperties.getTopicDeprecatedWords());

        stringProducer.close();
    }

    public void sendDeprecatedWord(String deprecatedWord) {
        KafkaProducer<String, String> stringProducer = getStringProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>(kafkaProperties.getTopicDeprecatedWords(),
                deprecatedWord, deprecatedWord);
        stringProducer.send(record);
        log.info("Запрещённое слово {} успешно отправлено в топик {}", record.value(),
                kafkaProperties.getTopicDeprecatedWords());
        stringProducer.close();
    }

    private String getMessage() {
        int randomNumber = getRandomNumber(RANDOM_WORDS.size());
        StringBuilder stringBuilder = new StringBuilder();
        for (int j = 0; j < randomNumber; j++) {
            stringBuilder.append(RANDOM_WORDS.get(j)).append(" ");
        }
        return stringBuilder.toString();
    }

    private KafkaProducer<Integer, MyMessage> getMyMessageProducer() {
        Properties properties = getCommonProducerProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MyMessageSerializer.class.getName());

        // Создание продюсера
        return new KafkaProducer<>(properties);
    }

    private KafkaProducer<String, String> getStringProducer() {
        Properties properties = getCommonProducerProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Создание продюсера
        return new KafkaProducer<>(properties);
    }

    private Properties getCommonProducerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());

        // Синхронная репликация - требует, чтобы все реплики синхронно подтвердили получение сообщения,
        // только после этого оно считается успешно отправленным
        properties.put(ProducerConfig.ACKS_CONFIG, kafkaProperties.getAcks());
        // Количество повторных попыток при отправке сообщений, если возникает ошибка.
        // Если три раза произошли ошибки, то сообщение считается неотправленным и ошибка будет возвращена продюсеру.
        properties.put(ProducerConfig.RETRIES_CONFIG, kafkaProperties.getRetries());
        // Минимум 2 реплики должны подтвердить запись
        properties.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, kafkaProperties.getReplicas());

        return properties;
    }

    private int getRandomNumber(int max) {
        Random random = new Random();
        return random.nextInt(max - 1) + 1;
    }
}
