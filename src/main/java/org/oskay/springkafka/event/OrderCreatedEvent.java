package org.oskay.springkafka.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

@Getter
@AllArgsConstructor
@NoArgsConstructor

public class OrderCreatedEvent {
    private UUID eventId;
    private Long orderId;

    private Long customerId;
    private BigDecimal totalPrice;

    @Override
    public String toString() {
        return "OrderCreatedEvent{" +
                "orderId=" + orderId +
                ", customerId=" + customerId +
                ", totalPrice=" + totalPrice +
                '}';
    }
}
