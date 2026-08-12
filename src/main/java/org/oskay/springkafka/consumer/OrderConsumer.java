package org.oskay.springkafka.consumer;

import org.oskay.springkafka.entity.ProcessedEvent;
import org.oskay.springkafka.event.OrderCreatedEvent;
import org.oskay.springkafka.repository.ProcessedEventRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
public class OrderConsumer {

    private final ProcessedEventRepository processedEventRepository;

    public OrderConsumer(
            ProcessedEventRepository processedEventRepository
    ) {
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    @KafkaListener(
            topics = "order-created",
            groupId = "order-group"
    )
    public void consume(OrderCreatedEvent event) {

        boolean alreadyProcessed =
                processedEventRepository.existsById(event.getEventId());

        if (alreadyProcessed) {
            System.out.println(
                    "Event daha önce işlendi, tekrar atlanıyor: "
                            + event.getEventId()
            );
            return;
        }

        System.out.println("Mesaj işleniyor: " + event);


        ProcessedEvent processedEvent = new ProcessedEvent(
                event.getEventId(),
                LocalDateTime.now()
        );

        processedEventRepository.save(processedEvent);

        System.out.println(
                "Event başarıyla işlendi: " + event.getEventId()
        );
    }
}