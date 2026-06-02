package com.retailmedia.insights.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.*;
import java.util.Map;

/**
 * Ad Event — core event flowing through the platform.
 *
 * PRIVACY: userId is SHA-256(loyaltyCardId + salt) — never raw PII.
 *
 * TIMESTAMP: Use event time from client, NOT server arrival time.
 * Mobile events arrive late (offline → reconnects). Flink uses
 * this timestamp for correct attribution window calculation.
 *
 * EVENT TYPES: IMPRESSION | CLICK | PRODUCT_VIEW | ADD_TO_CART | PURCHASE
 */
@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class AdEvent {

    @NotBlank @JsonProperty("eventId")    private String eventId;   // UUID for dedup
    @NotBlank @JsonProperty("eventType")  private String eventType;
    @JsonProperty("tenantId")             private String tenantId;   // set by server from header
    @NotBlank @JsonProperty("campaignId") private String campaignId;
    @JsonProperty("adId")                 private String adId;
    @JsonProperty("userId")               private String userId;     // hashed, never raw PII
    @JsonProperty("sessionId")            private String sessionId;
    @JsonProperty("productId")            private String productId;
    @JsonProperty("placementId")          private String placementId;

    @NotNull @Positive
    @JsonProperty("timestamp")            private Long timestamp;    // event time epoch ms

    @JsonProperty("metadata")             private Map<String, String> metadata;
}
