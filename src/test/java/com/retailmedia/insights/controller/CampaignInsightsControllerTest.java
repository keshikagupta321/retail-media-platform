package com.retailmedia.insights.controller;

import com.retailmedia.insights.dto.CampaignMetricsDTO;
import com.retailmedia.insights.dto.MetricResponseDTO;
import com.retailmedia.insights.exception.CampaignNotFoundException;
import com.retailmedia.insights.service.CampaignInsightsService;
import com.retailmedia.insights.service.EventCollectorService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@WebFluxTest(CampaignInsightsController.class)
class CampaignInsightsControllerTest {

    private static final String TENANT   = "tesco_uk";
    private static final String CAMPAIGN = "campaign_summer_2025";
    private static final String HDR      = "X-Tenant-Id";

    @Autowired WebTestClient client;
    @MockBean  CampaignInsightsService service;
    @MockBean  EventCollectorService eventService;

    @Nested @DisplayName("GET /clicks")
    class Clicks {

        @Test @DisplayName("200 with click count — real-time from Redis")
        void returnsCount() {
            when(service.getClicks(TENANT, CAMPAIGN, null, null)).thenReturn(Mono.just(1500L));

            client.get().uri("/api/v1/ad/{c}/clicks", CAMPAIGN).header(HDR, TENANT)
                    .exchange().expectStatus().isOk()
                    .expectBody(MetricResponseDTO.class)
                    .value(r -> {
                        assertThat(r.getMetric()).isEqualTo("clicks");
                        assertThat(r.getValue()).isEqualTo(1500);
                        assertThat(r.getSource()).isEqualTo("realtime-cache");
                    });
        }

        @Test @DisplayName("404 when campaign not found")
        void notFound() {
            when(service.getClicks(any(), any(), any(), any()))
                    .thenReturn(Mono.error(new CampaignNotFoundException(CAMPAIGN, TENANT)));
            client.get().uri("/api/v1/ad/{c}/clicks", CAMPAIGN).header(HDR, TENANT)
                    .exchange().expectStatus().isNotFound();
        }

        @Test @DisplayName("historical source when time range given")
        void historicalSource() {
            when(service.getClicks(eq(TENANT), eq(CAMPAIGN), any(), any()))
                    .thenReturn(Mono.just(50000L));
            client.get()
                    .uri("/api/v1/ad/{c}/clicks?from=2025-06-01T00:00:00&to=2025-06-30T23:59:59", CAMPAIGN)
                    .header(HDR, TENANT).exchange().expectStatus().isOk()
                    .expectBody(MetricResponseDTO.class)
                    .value(r -> assertThat(r.getSource()).isEqualTo("historical-druid"));
        }

        @Test @DisplayName("400 when tenant header missing")
        void missingTenant() {
            client.get().uri("/api/v1/ad/{c}/clicks", CAMPAIGN)
                    .exchange().expectStatus().isBadRequest();
        }
    }

    @Nested @DisplayName("GET /impressions")
    class Impressions {
        @Test @DisplayName("200 with impression count")
        void returnsCount() {
            when(service.getImpressions(TENANT, CAMPAIGN, null, null)).thenReturn(Mono.just(50000L));
            client.get().uri("/api/v1/ad/{c}/impressions", CAMPAIGN).header(HDR, TENANT)
                    .exchange().expectStatus().isOk()
                    .expectBody(MetricResponseDTO.class)
                    .value(r -> assertThat(r.getMetric()).isEqualTo("impressions"));
        }
    }

    @Nested @DisplayName("GET /clickToBasket")
    class ClickToBasket {
        @Test @DisplayName("200 with attribution count")
        void returnsCount() {
            when(service.getClickToBasket(TENANT, CAMPAIGN, null, null)).thenReturn(Mono.just(300L));
            client.get().uri("/api/v1/ad/{c}/clickToBasket", CAMPAIGN).header(HDR, TENANT)
                    .exchange().expectStatus().isOk()
                    .expectBody(MetricResponseDTO.class)
                    .value(r -> assertThat(r.getMetric()).isEqualTo("clickToBasket"));
        }
    }

    @Nested @DisplayName("GET /performance")
    class Performance {
        @Test @DisplayName("200 with CTR and conversion rate computed")
        void returnsAll() {
            CampaignMetricsDTO dto = CampaignMetricsDTO.builder()
                    .campaignId(CAMPAIGN).impressions(50000L).clicks(1500L)
                    .clickToBasket(300L).ctr(3.0).conversionRate(20.0).build();

            when(service.getCampaignPerformance(TENANT, CAMPAIGN, null, null))
                    .thenReturn(Mono.just(dto));

            client.get().uri("/api/v1/ad/{c}/performance", CAMPAIGN).header(HDR, TENANT)
                    .exchange().expectStatus().isOk()
                    .expectBody(MetricResponseDTO.class)
                    .value(r -> assertThat(r.getMetric()).isEqualTo("performance"));
        }
    }
}
