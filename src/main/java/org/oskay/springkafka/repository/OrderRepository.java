package org.oskay.springkafka.repository;



import org.oskay.springkafka.entity.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {
}