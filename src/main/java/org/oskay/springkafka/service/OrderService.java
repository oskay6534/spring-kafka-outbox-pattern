package org.oskay.springkafka.service;

import org.oskay.springkafka.entity.Order;
import org.oskay.springkafka.entity.OutboxEvent;
import org.oskay.springkafka.event.OrderCreatedEvent;
import org.oskay.springkafka.repository.OrderRepository;
import org.oskay.springkafka.repository.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OrderService(
            OrderRepository orderRepository,
            OutboxEventRepository outboxEventRepository,
            ObjectMapper objectMapper
    ) {
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Long createOrder(
            Long customerId,
            BigDecimal totalPrice
    ) {
        Order order = new Order(customerId, totalPrice);

        orderRepository.save(order);

        UUID eventId = UUID.randomUUID();

        OrderCreatedEvent event = new OrderCreatedEvent(
                eventId,
                order.getId(),
                customerId,
                totalPrice
        );

        String payload = toJson(event);

        OutboxEvent outboxEvent = new OutboxEvent(
                eventId,
                "ORDER",
                order.getId(),
                "ORDER_CREATED",
                payload,
                "NEW",
                LocalDateTime.now()
        );

        outboxEventRepository.save(outboxEvent);

        return order.getId();
    }

    private String toJson(OrderCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JacksonException exception) {
            throw new IllegalStateException(
                    "OrderCreatedEvent JSON'a çevrilemedi.",
                    exception
            );
        }
    }
}