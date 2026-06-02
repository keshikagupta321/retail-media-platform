CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id              VARCHAR(100) NOT NULL,
    name                   VARCHAR(255),
    status                 VARCHAR(20) DEFAULT 'ACTIVE',
    attribution_window_min INT DEFAULT 30,
    created_at             TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_campaigns_tenant ON campaigns(tenant_id);

CREATE TABLE IF NOT EXISTS tenant_configs (
    tenant_id                VARCHAR(100) PRIMARY KEY,
    tier                     VARCHAR(20) DEFAULT 'STANDARD',
    max_events_per_second    INT DEFAULT 10000,
    kafka_partition_count    INT DEFAULT 10,
    data_retention_days      INT DEFAULT 90,
    attribution_window_min   INT DEFAULT 30
);

INSERT INTO tenant_configs VALUES
('retailer_test','STANDARD',10000,10,90,30)
ON CONFLICT DO NOTHING;

INSERT INTO campaigns (campaign_id, tenant_id, name)
VALUES ('campaign_summer_2025','retailer_test','Summer 2025')
ON CONFLICT DO NOTHING;
