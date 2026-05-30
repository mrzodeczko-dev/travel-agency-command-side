package com.rzodeczko.infrastructure.kafka.outbox;


import tools.jackson.databind.ObjectMapper;
import com.rzodeczko.domain.model.Booking;
import com.rzodeczko.infrastructure.kafka.avro.BookingCreatedAvro;
import com.rzodeczko.infrastructure.kafka.properties.KafkaTopicProperties;
import com.rzodeczko.infrastructure.kafka.properties.OutboxProperties;
import com.rzodeczko.infrastructure.persistence.entity.DeadLetterEntity;
import com.rzodeczko.infrastructure.persistence.entity.OutboxEntity;
import com.rzodeczko.infrastructure.persistence.repository.JpaDeadLetterRepository;
import com.rzodeczko.infrastructure.persistence.repository.JpaOutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxScheduler {
    private final JpaOutboxRepository jpaOutboxRepository;
    private final JpaDeadLetterRepository jpaDeadLetterRepository;

    private final KafkaTopicProperties kafkaTopicProperties;
    private final OutboxProperties outboxProperties;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;

    @Scheduled(fixedDelayString = "${kafka.outbox.poll-interval}")
    public void processOutbox() {
        List<OutboxEntity> entries = jpaOutboxRepository.findAllByOrderByCreatedAtAsc(
                PageRequest.of(0, outboxProperties.batchSize()));

        for (OutboxEntity entry : entries) {
            try {
                sendToKafka(entry);
                jpaOutboxRepository.delete(entry);
            } catch (Exception e) {
                handleFailure(entry, e);
                break;
            }
        }
    }

    private void sendToKafka(OutboxEntity entry) {
        SpecificRecordBase avro = toAvro(entry);
        var record = new ProducerRecord<>(
                kafkaTopicProperties.name(),
                entry.getAggregateId(),
                avro
        );
        record.headers().add(
                "eventType",
                entry.getType().getBytes(StandardCharsets.UTF_8)
        );
        kafkaTemplate.send(record);
    }

    private void handleFailure(OutboxEntity entry, Exception e) {
        entry.incrementRetryCount();
        if (entry.hasExceededMaxRetries(outboxProperties.maxRetries())) {
            moveToDeadLetter(entry, e);
            jpaOutboxRepository.delete(entry);
            log.error("Outbox entry {} moved to DLT after {} retries: {}",
                    entry.getId(), outboxProperties.maxRetries(), e.getMessage());
        } else {
            jpaOutboxRepository.save(entry);
            log.warn("Outbox entry {} failed (attempt {}/{}): {}",
                    entry.getId(), entry.getRetryCount(), outboxProperties.maxRetries(), e.getMessage());
        }
    }

    private void moveToDeadLetter(OutboxEntity entry, Exception e) {
        var deadLetter = DeadLetterEntity.builder()
                .originalOutboxId(entry.getId())
                .aggregateId(entry.getAggregateId())
                .type(entry.getType())
                .payload(entry.getPayload())
                .errorMessage(e.getMessage())
                .createdAt(entry.getCreatedAt())
                .failedAt(LocalDateTime.now())
                .retryCount(entry.getRetryCount())
                .build();
        jpaDeadLetterRepository.save(deadLetter);
    }

    private SpecificRecordBase toAvro(OutboxEntity entry) {
        try {
            Booking booking = objectMapper.readValue(entry.getPayload(), Booking.class);
            return switch (entry.getType()) {
                case "BookingCreated" -> BookingCreatedAvro.newBuilder()
                        .setId(booking.id())
                        .setHotelId(booking.hotelId())
                        .setUserId(booking.userId())
                        .setStart(booking.start().toString())
                        .setEnd(booking.end().toString())
                        .build();
                default -> throw new IllegalArgumentException("Unknown event type: " + entry.getType());
            };
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert outbox payload to AVRO");
        }
    }
}
