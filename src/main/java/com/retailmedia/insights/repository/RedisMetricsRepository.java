package com.retailmedia.insights.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/**
 * Redis repository for real-time campaign metrics.
 *
 * KEY SCHEMA (tenant-scoped — prevents cross-tenant access):
 *   {tenantId}:campaign:{campaignId}:impressions → Long
 *   {tenantId}:campaign:{campaignId}:clicks      → Long
 *   {tenantId}:campaign:{campaignId}:click_to_basket → Long
 *
 * WHY ATOMIC:
 *   Redis INCR is single-threaded at server level.
 *   20 API pods all calling INCR on same key = all counted exactly once.
 *   No locks, no transactions needed. This is Redis's core guarantee.
 *
 * TTL: 25 hours — covers rolling 24h window with 1h buffer.
 *      Counters reset by scheduled job at midnight per tenant timezone.
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class RedisMetricsRepository {

    private final ReactiveRedisTemplate<String, Long> redisTemplate;
    private static final String KEY = "%s:campaign:%s:%s";

    public Mono<Long> getMetric(String tenantId, String campaignId, String metric) {
        return redisTemplate.opsForValue().get(key(tenantId, campaignId, metric));
    }

    public Mono<Long> incrementMetric(String tenantId, String campaignId, String metric) {
        return redisTemplate.opsForValue().increment(key(tenantId, campaignId, metric));
    }

    private String key(String t, String c, String m) {
        return String.format(KEY, t, c, m);
    }
}
