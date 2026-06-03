package com.retailmedia.insights.service;

import com.retailmedia.insights.event.AdEvent;
import com.retailmedia.insights.repository.RedisMetricsRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Event Collector Service — ingestion pipeline.
 *
 * Pipeline per event:
 *   1. Redis SETNX dedup (eventId, 24h TTL)
 *   2. Increment real-time counter (atomic INCR)
 *   3. Publish to Kafka (idempotent producer)
 *
 * THREE LAYERS OF DEDUPLICATION:
 *   1. Redis SETNX — catches client retries within 24h
 *   2. Kafka idempotent producer — catches producer retries
 *      (enable.idempotence=true, broker deduplicates by PID+sequence)
 *   3. Druid rollup at ingestion — SUM aggregation naturally idempotent
 *
 * KAFKA TOPIC: {tenantId}.ad.events
 * PARTITION KEY: userId — guarantees per-user event ordering.
 * Why: attribution requires CLICK before ADD_TO_CART in order.
 * Same user → same partition → same consumer → in-order processing.
 */
@Service
@Slf4j
public class EventCollectorService {

    private final ReactiveRedisTemplate<String, String> stringRedisTemplate;
    private final RedisMetricsRepository redisMetricsRepo;
    private final KafkaTemplate<String, AdEvent> kafkaTemplate;

    public EventCollectorService(
            @Qualifier("dedupRedisTemplate") ReactiveRedisTemplate<String, String> stringRedisTemplate,
            RedisMetricsRepository redisMetricsRepo,
            KafkaTemplate<String, AdEvent> kafkaTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.redisMetricsRepo = redisMetricsRepo;
        this.kafkaTemplate = kafkaTemplate;
    }

    private static final Duration DEDUP_TTL = Duration.ofHours(24);
    private static final String DEDUP_PREFIX = "dedup:event:";

    /**
     * Process an incoming ad event.
     *
     * @return true = new event processed
     *         false = duplicate, idempotently ignored (not an error)
     */
    public Mono<Boolean> processEvent(AdEvent event, String tenantId) {
        String dedupKey = DEDUP_PREFIX + event.getEventId();

        // SETNX = SET if Not eXists — atomic Redis operation
        // Returns true if key was set (new event, process it)
        // Returns false if key existed (duplicate, skip silently)
        return stringRedisTemplate.opsForValue()
                .setIfAbsent(dedupKey, "1", DEDUP_TTL)
                .flatMap(isNew -> {
                    if (Boolean.FALSE.equals(isNew)) {
                        log.debug("Duplicate event | id={}", event.getEventId());
                        return Mono.just(false);
                    }
                    // New event: increment counter then publish to Kafka
                    return incrementCounter(tenantId, event)
                            .then(publishToKafka(tenantId, event))
                            .thenReturn(true);
                });
    }

    private Mono<Long> incrementCounter(String tenantId, AdEvent event) {
        String metric = switch (event.getEventType()) {
            case "IMPRESSION"  -> "impressions";
            case "CLICK"       -> "clicks";
            case "ADD_TO_CART" -> "cart_adds";
            default            -> "other";
        };
        return redisMetricsRepo.incrementMetric(tenantId, event.getCampaignId(), metric);
    }

    private Mono<Void> publishToKafka(String tenantId, AdEvent event) {
        String topic = tenantId + ".ad.events";
        // Partition by userId — per-user ordering for attribution
        return Mono.fromFuture(
                kafkaTemplate.send(topic, event.getUserId(), event).toCompletableFuture()
        ).then();
    }
}
