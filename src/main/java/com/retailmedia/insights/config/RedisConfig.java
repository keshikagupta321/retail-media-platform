package com.retailmedia.insights.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.serializer.GenericToStringSerializer;

/**
 * Redis configuration.
 *
 * Two templates:
 *   ReactiveRedisTemplate<String, Long>  → metric counters (INCR)
 *   ReactiveRedisTemplate<String, String> → dedup keys (SETNX)
 */
@Configuration
public class RedisConfig {

    /**
     * Template for Long values — used by RedisMetricsRepository
     * for atomic INCR counters (impressions, clicks, click_to_basket).
     */
    @Bean
    public ReactiveRedisTemplate<String, Long> longRedisTemplate(
            ReactiveRedisConnectionFactory factory) {

        RedisSerializationContext<String, Long> context =
                RedisSerializationContext.<String, Long>newSerializationContext(
                        new StringRedisSerializer())
                        .value(new GenericToStringSerializer<>(Long.class))
                        .build();

        return new ReactiveRedisTemplate<>(factory, context);
    }

    /**
     * Template for String values — used by EventCollectorService
     * for SETNX deduplication keys.
     */
    @Bean("dedupRedisTemplate")
    public ReactiveRedisTemplate<String, String> dedupRedisTemplate(
            ReactiveRedisConnectionFactory factory) {

        RedisSerializationContext<String, String> context =
                RedisSerializationContext.<String, String>newSerializationContext(
                        new StringRedisSerializer())
                        .value(new StringRedisSerializer())
                        .build();

        return new ReactiveRedisTemplate<>(factory, context);
    }
}
