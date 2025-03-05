package yandex.praktikum.kafka.dto;

import lombok.Builder;
import lombok.Getter;
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

@Getter
@Builder
public class BlockedUsers {

    // Имя пользователя, для которого нужно заблокировать пользователей
    private String user;
    // Cписок пользователей, сообщения от которых будут заблокированы
    private List<String> blockedUsers;

}
