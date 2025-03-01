package yandex.praktikum.kafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;
import yandex.praktikum.kafka.config.KafkaProperties;
import yandex.praktikum.kafka.dto.MyMessage;
import yandex.praktikum.kafka.dto.MyMessageSerializer;

import javax.annotation.PostConstruct;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class Producer {

    private final KafkaProperties kafkaProperties;

    @PostConstruct
    public void init() {
        // Запуск продьюсера
        ExecutorService producerExecutorService = Executors.newSingleThreadExecutor();
        producerExecutorService.submit(this::send);
    }

    private void send() {
        KafkaProducer<Integer, MyMessage> myMessageProducer = getMyMessageProducer();

        // Отправка 10 сообщений в топик
        for (int i = 0; i < 10; i++) {
            var myMessage = MyMessage.builder()
                    .author("user" + "-" + i)
                    .message("Hello")
                    .recipient("recipient" + "-" + i)
                    .build();
            ProducerRecord<Integer, MyMessage> record = new ProducerRecord<>("messages", i, myMessage);
            myMessageProducer.send(record);
            log.info("Message {} was successfully sent in topic messages", record.value());
        }

        KafkaProducer<String, String> stringProducer = getStringProducer();
        // Имя пользователя напротив списка пользователей, сообщения от которых будут зблокированы
        Map<String, List<String>> blockedUsersMap = getBlockedUsers();
        blockedUsersMap.keySet().forEach(key -> {
            List<String> blockedUsers = blockedUsersMap.get(key);
            blockedUsers.forEach(blockedUser -> {
                ProducerRecord<String, String> record = new ProducerRecord<>("blocked-users", key, blockedUser);
                stringProducer.send(record);
                log.info("Message {} was successfully sent in topic blocked-users", record.value());
            });
        });

        // Закрытие продюсера
        myMessageProducer.close();
        stringProducer.close();
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

    @SneakyThrows
    private Map<String, List<String>> getBlockedUsers() {
        Map<String, List<String>> blockedUsersMap = new HashMap<>();
        try (InputStream inputStream = Producer.class.getResourceAsStream("blocked_user1.txt")) {
            String blockedUsers = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            String[] strings = blockedUsers.split(";");
            blockedUsersMap.put("user-1", Arrays.stream(strings).toList());
        }
        return blockedUsersMap;
    }
}
