package com.retailmedia.insights.controller;

import com.retailmedia.insights.dto.CampaignMetricsDTO;
import com.retailmedia.insights.dto.MetricResponseDTO;
import com.retailmedia.insights.event.AdEvent;
import com.retailmedia.insights.exception.CampaignNotFoundException;
import com.retailmedia.insights.service.CampaignInsightsService;
import com.retailmedia.insights.service.EventCollectorService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

/**
 * Campaign Insights API.
 *
 * Query modes:
 *   Real-time (no time params) → Redis, sub-millisecond, rolling 1h
 *   Historical (from + to)     → Druid, sub-second, up to 90 days
 *
 * Multi-tenancy: X-Tenant-Id header extracted from JWT by Gateway.
 * Every query auto-scoped. No cross-tenant access possible.
 */
@RestController
@RequiredArgsConstructor
@Validated
@Slf4j
@Tag(name = "Campaign Insights")
public class CampaignInsightsController {

    private final CampaignInsightsService insightsService;
    private final EventCollectorService eventCollectorService;

    // ─── Event Ingestion ────────────────────────────────────────────────

    /**
     * POST /api/v1/events — Ingest an ad event.
     * Validates → deduplicates → increments Redis counter → publishes to Kafka.
     */
    @PostMapping("/api/v1/events")
    @Operation(summary = "Ingest an ad event")
    public Mono<ResponseEntity<Void>> ingestEvent(
            @Valid @RequestBody AdEvent event,
            @RequestHeader("X-Tenant-Id") String tenantId) {

        event.setTenantId(tenantId);
        log.info("Event received | type={} campaign={} tenant={}",
                event.getEventType(), event.getCampaignId(), tenantId);

        return eventCollectorService.processEvent(event, tenantId)
                .map(processed -> processed
                        ? ResponseEntity.status(HttpStatus.ACCEPTED).<Void>build()
                        : ResponseEntity.ok().<Void>build()); // 200 = duplicate, accepted idempotently
    }

    // ─── Insights Queries ────────────────────────────────────────────────

    /**
     * GET /api/v1/ad/{campaignId}/clicks
     * Number of customers who clicked the ad.
     * Real-time → Redis. Historical → Druid.
     */
    @GetMapping("/api/v1/ad/{campaignId}/clicks")
    @Operation(summary = "Get click count")
    public Mono<ResponseEntity<MetricResponseDTO<Long>>> getClicks(
            @PathVariable @NotBlank String campaignId,
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {

        log.info("GET clicks | tenant={} campaign={} realtime={}", tenantId, campaignId, from == null);
        return insightsService.getClicks(tenantId, campaignId, from, to)
                .map(v -> ResponseEntity.ok(MetricResponseDTO.of("clicks", v, campaignId, from, to)))
                .onErrorResume(CampaignNotFoundException.class,
                        e -> Mono.just(ResponseEntity.notFound().build()));
    }

    /**
     * GET /api/v1/ad/{campaignId}/impressions
     * Number of times the ad was displayed.
     */
    @GetMapping("/api/v1/ad/{campaignId}/impressions")
    @Operation(summary = "Get impression count")
    public Mono<ResponseEntity<MetricResponseDTO<Long>>> getImpressions(
            @PathVariable @NotBlank String campaignId,
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {

        return insightsService.getImpressions(tenantId, campaignId, from, to)
                .map(v -> ResponseEntity.ok(MetricResponseDTO.of("impressions", v, campaignId, from, to)))
                .onErrorResume(CampaignNotFoundException.class,
                        e -> Mono.just(ResponseEntity.notFound().build()));
    }

    /**
     * GET /api/v1/ad/{campaignId}/clickToBasket
     * Customers who added to cart within attribution window after clicking.
     * Flink session windows + exactly-once = never double-counted.
     */
    @GetMapping("/api/v1/ad/{campaignId}/clickToBasket")
    @Operation(summary = "Get click-to-basket conversions")
    public Mono<ResponseEntity<MetricResponseDTO<Long>>> getClickToBasket(
            @PathVariable @NotBlank String campaignId,
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {

        return insightsService.getClickToBasket(tenantId, campaignId, from, to)
                .map(v -> ResponseEntity.ok(MetricResponseDTO.of("clickToBasket", v, campaignId, from, to)))
                .onErrorResume(CampaignNotFoundException.class,
                        e -> Mono.just(ResponseEntity.notFound().build()));
    }

    /**
     * GET /api/v1/ad/{campaignId}/performance
     * All metrics in one call with CTR and conversion rate.
     * Use for dashboards — avoids 3 round trips.
     * Fetches impressions + clicks + clickToBasket in parallel (Mono.zip).
     */
    @GetMapping("/api/v1/ad/{campaignId}/performance")
    @Operation(summary = "Get full performance summary")
    public Mono<ResponseEntity<MetricResponseDTO<CampaignMetricsDTO>>> getPerformance(
            @PathVariable @NotBlank String campaignId,
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {

        return insightsService.getCampaignPerformance(tenantId, campaignId, from, to)
                .map(v -> ResponseEntity.ok(MetricResponseDTO.of("performance", v, campaignId, from, to)))
                .onErrorResume(CampaignNotFoundException.class,
                        e -> Mono.just(ResponseEntity.notFound().build()));
    }
}
