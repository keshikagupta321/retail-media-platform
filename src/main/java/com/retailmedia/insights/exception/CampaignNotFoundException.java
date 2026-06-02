package com.retailmedia.insights.exception;

public class CampaignNotFoundException extends RuntimeException {
    public CampaignNotFoundException(String campaignId, String tenantId) {
        super(String.format("Campaign '%s' not found for tenant '%s'",
                campaignId, tenantId));
    }
}
