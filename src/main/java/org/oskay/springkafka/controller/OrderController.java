package org.oskay.springkafka.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.oskay.springkafka.service.OrderService;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

@RestController
@RequestMapping("/orders")
@Tag(name = "Orders", description = "Sipariş oluşturma işlemleri")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    @Operation(summary = "Sipariş oluşturur ve outbox event kaydı ekler")
    public String createOrder() {

        Long orderId = orderService.createOrder(
                15L,
                new BigDecimal("250.75")
        );

        return "Order oluşturuldu. ID: " + orderId;
    }
}
