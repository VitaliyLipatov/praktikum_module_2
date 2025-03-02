package yandex.praktikum.kafka;

import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import yandex.praktikum.kafka.config.KafkaProperties;

@EnableScheduling
@EnableConfigurationProperties(KafkaProperties.class)
@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) {
        new SpringApplicationBuilder(KafkaApplication.class)
                .bannerMode(Banner.Mode.OFF)
                .run(args);
    }
}
