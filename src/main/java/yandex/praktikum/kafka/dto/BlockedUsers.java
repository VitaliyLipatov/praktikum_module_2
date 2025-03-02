package yandex.praktikum.kafka.dto;

import lombok.SneakyThrows;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.stereotype.Component;
import yandex.praktikum.kafka.producer.Producer;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class BlockedUsers {

    // Имя пользователя напротив списка пользователей, сообщения от которых будут заблокированы
    private final Map<String, List<String>> blockedUsersMap = new HashMap<>();

    @SneakyThrows
    public Map<String, List<String>> getBlockedUsers(String userName, String fileName) {
        try (InputStream inputStream = BlockedUsers.class.getResourceAsStream(fileName)) {
            String blockedUsers = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            String[] strings = blockedUsers.split(";");
            blockedUsersMap.put(userName, Arrays.stream(strings).toList());
        }
        return blockedUsersMap;
    }
}
