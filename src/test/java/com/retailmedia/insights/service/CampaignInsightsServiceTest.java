package com.retailmedia.insights.service;

import com.retailmedia.insights.repository.DruidMetricsRepository;
import com.retailmedia.insights.repository.RedisMetricsRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CampaignInsightsServiceTest {

    @Mock RedisMetricsRepository redis;
    @Mock DruidMetricsRepository druid;
    @InjectMocks CampaignInsightsService service;

    static final String T = "tesco_uk", C = "camp_summer";

    @Test @DisplayName("Real-time clicks served from Redis — no Druid call")
    void clicksFromRedis() {
        when(redis.getMetric(T, C, "clicks")).thenReturn(Mono.just(1500L));

        StepVerifier.create(service.getClicks(T, C, null, null))
                .expectNext(1500L).verifyComplete();

        verify(redis).getMetric(T, C, "clicks");
        verify(druid, never()).queryMetric(any(), any(), any(), any(), any());
    }

    @Test @DisplayName("Historical clicks served from Druid — no Redis call")
    void clicksFromDruid() {
        LocalDateTime from = LocalDateTime.of(2025, 6, 1, 0, 0);
        LocalDateTime to   = LocalDateTime.of(2025, 6, 30, 23, 59);
        when(druid.queryMetric(T, C, "click_count", from, to)).thenReturn(Mono.just(75000L));

        StepVerifier.create(service.getClicks(T, C, from, to))
                .expectNext(75000L).verifyComplete();

        verify(druid).queryMetric(T, C, "click_count", from, to);
        verify(redis, never()).getMetric(any(), any(), any());
    }

    @Test @DisplayName("Performance computes CTR and conversion rate correctly")
    void performanceMetrics() {
        when(redis.getMetric(T, C, "impressions")).thenReturn(Mono.just(50000L));
        when(redis.getMetric(T, C, "clicks")).thenReturn(Mono.just(1500L));
        when(redis.getMetric(T, C, "click_to_basket")).thenReturn(Mono.just(300L));

        StepVerifier.create(service.getCampaignPerformance(T, C, null, null))
                .assertNext(m -> {
                    assertThat(m.getImpressions()).isEqualTo(50000L);
                    assertThat(m.getClicks()).isEqualTo(1500L);
                    assertThat(m.getClickToBasket()).isEqualTo(300L);
                    assertThat(m.getCtr()).isEqualTo(3.0);           // 1500/50000*100
                    assertThat(m.getConversionRate()).isEqualTo(20.0); // 300/1500*100
                }).verifyComplete();
    }

    @Test @DisplayName("CTR is zero when impressions are zero — no divide by zero")
    void noDivideByZero() {
        when(redis.getMetric(T, C, "impressions")).thenReturn(Mono.just(0L));
        when(redis.getMetric(T, C, "clicks")).thenReturn(Mono.just(0L));
        when(redis.getMetric(T, C, "click_to_basket")).thenReturn(Mono.just(0L));

        StepVerifier.create(service.getCampaignPerformance(T, C, null, null))
                .assertNext(m -> {
                    assertThat(m.getCtr()).isEqualTo(0.0);
                    assertThat(m.getConversionRate()).isEqualTo(0.0);
                }).verifyComplete();
    }

    @Test @DisplayName("Returns zero for new campaign with no Redis key")
    void zeroForMissingKey() {
        when(redis.getMetric(T, C, "clicks")).thenReturn(Mono.empty());

        StepVerifier.create(service.getClicks(T, C, null, null))
                .expectNext(0L).verifyComplete();
    }
}
