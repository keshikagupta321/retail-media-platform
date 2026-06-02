package com.retailmedia.insights.service;

import com.retailmedia.insights.dto.CampaignMetricsDTO;
import com.retailmedia.insights.repository.DruidMetricsRepository;
import com.retailmedia.insights.repository.RedisMetricsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

/**
 * Campaign Insights Service.
 *
 * ROUTING LOGIC:
 *   No time range  → Redis  (sub-millisecond, rolling 1-hour window)
 *   With time range → Druid (sub-second, up to 90 days)
 *
 * PERFORMANCE ENDPOINT:
 *   Uses Mono.zip to fetch impressions + clicks + clickToBasket in PARALLEL.
 *   Sequential: 3 x 1ms (Redis) = 3ms total.
 *   Parallel:   max(1ms, 1ms, 1ms) = 1ms total.
 *   With Druid: 3 x 100ms sequential = 300ms → 100ms with Mono.zip.
 *   3x faster for the most-used dashboard endpoint.
 *
 * CTR and conversion rate computed server-side to avoid client calculation errors.
 * All methods tenant-scoped — tenantId in every Redis key and Druid datasource name.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class CampaignInsightsService {

    private final RedisMetricsRepository redisRepo;
    private final DruidMetricsRepository druidRepo;

    public Mono<Long> getClicks(String tenantId, String campaignId,
                                 LocalDateTime from, LocalDateTime to) {
        if (isRealTime(from, to)) {
            log.debug("Clicks from Redis | tenant={} campaign={}", tenantId, campaignId);
            return redisRepo.getMetric(tenantId, campaignId, "clicks").defaultIfEmpty(0L);
        }
        log.debug("Clicks from Druid | tenant={} from={} to={}", tenantId, from, to);
        return druidRepo.queryMetric(tenantId, campaignId, "click_count", from, to);
    }

    public Mono<Long> getImpressions(String tenantId, String campaignId,
                                      LocalDateTime from, LocalDateTime to) {
        if (isRealTime(from, to)) {
            return redisRepo.getMetric(tenantId, campaignId, "impressions").defaultIfEmpty(0L);
        }
        return druidRepo.queryMetric(tenantId, campaignId, "impression_count", from, to);
    }

    /**
     * Click-to-basket attribution.
     *
     * Powered by Flink session window job:
     *   1. Keyed stream by userId + campaignId
     *   2. 30-minute session window (configurable per tenant)
     *   3. On ADD_TO_CART: look back in window for CLICK of same campaign
     *   4. If found → emit attribution event
     *   5. Exactly-once via Flink checkpoint 2PC to Iceberg
     *      AND Redis INCR update — never double-counted on restart
     *
     * Real-time: Redis counter updated by Flink attribution job
     * Historical: Druid click_to_basket_count column
     */
    public Mono<Long> getClickToBasket(String tenantId, String campaignId,
                                        LocalDateTime from, LocalDateTime to) {
        if (isRealTime(from, to)) {
            return redisRepo.getMetric(tenantId, campaignId, "click_to_basket").defaultIfEmpty(0L);
        }
        return druidRepo.queryMetric(tenantId, campaignId, "click_to_basket_count", from, to);
    }

    /**
     * All metrics in one call — parallel fetch via Mono.zip.
     *
     * Why Mono.zip not sequential flatMap:
     *   Sequential: getImpressions → then getClicks → then getClickToBasket
     *   Each adds latency. With Druid at 100ms each = 300ms total.
     *
     *   Mono.zip: all three run concurrently on the event loop.
     *   Total time = slowest of the three = ~100ms not 300ms.
     *   3x faster for the most commonly called endpoint.
     */
    public Mono<CampaignMetricsDTO> getCampaignPerformance(
            String tenantId, String campaignId,
            LocalDateTime from, LocalDateTime to) {

        return Mono.zip(
                getImpressions(tenantId, campaignId, from, to),
                getClicks(tenantId, campaignId, from, to),
                getClickToBasket(tenantId, campaignId, from, to)
        ).map(tuple -> {
            long impressions   = tuple.getT1();
            long clicks        = tuple.getT2();
            long clickToBasket = tuple.getT3();

            // Avoid divide by zero
            double ctr = impressions > 0
                    ? Math.round((double) clicks / impressions * 10000.0) / 100.0
                    : 0.0;

            double conversionRate = clicks > 0
                    ? Math.round((double) clickToBasket / clicks * 10000.0) / 100.0
                    : 0.0;

            return CampaignMetricsDTO.builder()
                    .campaignId(campaignId)
                    .impressions(impressions)
                    .clicks(clicks)
                    .clickToBasket(clickToBasket)
                    .ctr(ctr)
                    .conversionRate(conversionRate)
                    .build();
        });
    }

    private boolean isRealTime(LocalDateTime from, LocalDateTime to) {
        return from == null && to == null;
    }
}
