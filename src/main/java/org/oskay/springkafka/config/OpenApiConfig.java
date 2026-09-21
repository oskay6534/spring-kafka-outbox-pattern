package org.oskay.springkafka.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    OpenAPI transactionalOutboxOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Transactional Outbox Pattern API")
                .version("v1")
                .description("Spring Boot, Apache Kafka ve PostgreSQL ile sipariş event yönetimi."));
    }
}
