package org.oskay.springkafka.producer;

import org.oskay.springkafka.event.OrderCreatedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
//devre dıişı şu an
@Service
public class OrderProducer {

    private static final String TOPIC = "order-created";

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderProducer(
            KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate
    ) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(OrderCreatedEvent event) {

        String key = event.getOrderId().toString();

        CompletableFuture<SendResult<String, OrderCreatedEvent>> future =
                kafkaTemplate.send(TOPIC, key, event);

        future.whenComplete((result, exception) -> {

            if (exception != null) {
                System.err.println(
                        "Mesaj gönderilemedi: " + exception.getMessage()
                );
                return;
            }

            System.out.println("Mesaj başarıyla gönderildi.");

            System.out.println(
                    "Topic: " +
                            result.getRecordMetadata().topic()
            );

            System.out.println(
                    "Partition: " +
                            result.getRecordMetadata().partition()
            );

            System.out.println(
                    "Offset: " +
                            result.getRecordMetadata().offset()
            );
        });
    }
}