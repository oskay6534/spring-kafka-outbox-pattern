package org.oskay.springkafka.repository;

import org.oskay.springkafka.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository
        extends JpaRepository<OutboxEvent, UUID> {

    List<OutboxEvent> findByStatusOrderByCreatedAtAsc(String status);
    List<OutboxEvent>
    findTop100ByStatusOrderByCreatedAtAsc(String status); /*Yani yaklaşık şu SQL mantığı:

SELECT *
FROM outbox_events
WHERE status = 'NEW'
ORDER BY created_at ASC
LIMIT 100;*/

}