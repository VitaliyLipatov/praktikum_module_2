package yandex.praktikum.kafka;

import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import yandex.praktikum.kafka.config.KafkaProperties;

@EnableConfigurationProperties(KafkaProperties.class)
@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) {
        new SpringApplicationBuilder(KafkaApplication.class)
                .bannerMode(Banner.Mode.OFF)
                .run(args);
    }
}
