package com.retailmedia.insights.dto;

import lombok.Builder;
import lombok.Data;

@Data @Builder
public class CampaignMetricsDTO {
    private String campaignId;
    private long impressions;
    private long clicks;
    private long clickToBasket;
    private double ctr;            // clicks / impressions * 100
    private double conversionRate; // clickToBasket / clicks * 100
}
