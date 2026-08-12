package org.oskay.springkafka.publisher;

import org.oskay.springkafka.entity.OutboxEvent;
import org.oskay.springkafka.repository.OutboxEventRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class OutboxPublisher {

    private static final String TOPIC = "order-created";

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            KafkaTemplate<String, String> kafkaTemplate
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 5000)
    public void publishPendingEvents() {

        List<OutboxEvent> events =
                outboxEventRepository
                        .findTop100ByStatusOrderByCreatedAtAsc("NEW");

        for (OutboxEvent event : events) {
            publishEvent(event);
        }
    }

    private void publishEvent(OutboxEvent event) {

        String key = event.getAggregateId().toString();
        String payload = event.getPayload();

        try {
            SendResult<String, String> result =
                    kafkaTemplate
                            .send(TOPIC, key, payload)
                            .get(10, TimeUnit.SECONDS);

            event.markAsSent();
            outboxEventRepository.save(event);

            System.out.println(
                    "Outbox event gönderildi." +
                            " Event ID: " + event.getId() +
                            ", Partition: " +
                            result.getRecordMetadata().partition() +
                            ", Offset: " +
                            result.getRecordMetadata().offset()
            );

        } catch (Exception exception) {

            System.err.println(
                    "Outbox event gönderilemedi." +
                            " Event ID: " + event.getId() +
                            ", Hata: " + exception.getMessage()
            );
        }
    }
}