package com.retailmedia.insights.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;

/**
 * Druid HTTP client configuration.
 *
 * Druid exposes a SQL HTTP API at /druid/v2/sql.
 * We use Spring WebClient (non-blocking) to query it.
 *
 * Timeouts:
 *   Connect: 2s  — fail fast if Druid unreachable
 *   Read:    10s — Druid queries can take up to a few seconds
 *                  for large time ranges
 */
@Configuration
public class DruidConfig {

    @Value("${druid.url:http://localhost:8888}")
    private String druidUrl;

    @Bean
    public WebClient druidWebClient() {
        return WebClient.builder()
                .baseUrl(druidUrl)
                .defaultHeader("Content-Type", "application/json")
                .defaultHeader("Accept", "application/json")
                .codecs(config -> config
                        .defaultCodecs()
                        .maxInMemorySize(10 * 1024 * 1024)) // 10MB response limit
                .build();
    }
}
