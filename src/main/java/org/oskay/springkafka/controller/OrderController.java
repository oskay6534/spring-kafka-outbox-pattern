package org.oskay.springkafka.controller;

import org.oskay.springkafka.service.OrderService;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public String createOrder() {

        Long orderId = orderService.createOrder(
                15L,
                new BigDecimal("250.75")
        );

        return "Order oluşturuldu. ID: " + orderId;
    }
}