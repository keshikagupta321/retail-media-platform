# Real-Time Retail Media Analytics Platform

**Author: Keshika Gupta — Tech Lead, 8.5 years Java backend, IOT83**

> Built and run a similar pipeline in production: 72M IoT events/day,
> Fortune 500 clients, exactly-once semantics, sub-second Druid queries.
> This design applies the same engineering patterns to retail media.

---

## Table of Contents
1. [Architecture Overview](#1-architecture-overview)
2. [Technology Choices](#2-technology-choices--rationale)
3. [Event Schema](#3-event-schema)
4. [Data Storage Design](#4-data-storage-design)
5. [Insights API](#5-insights-api)
6. [Scalability Strategy](#6-scalability-strategy)
7. [Multi-Tenancy](#7-multi-tenancy)
8. [Deployment Overview](#8-deployment-overview)
9. [Metrics and Monitoring](#9-metrics-and-monitoring)
10. [Privacy and GDPR](#10-privacy-and-gdpr)
11. [Cost Analysis](#11-cost-analysis)
12. [Challenges and Trade-offs](#12-challenges-and-trade-offs)
13. [Local Setup](#13-local-setup)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         EVENT SOURCES                                │
│  Retailer Website (JS Pixel) │ Mobile App (SDK) │ CDN Logs          │
└─────────────────────────────────────────────────────────────────────┘
                               ↓ HTTPS
┌─────────────────────────────────────────────────────────────────────┐
│              AWS ALB — SSL │ WAF │ Geographic Routing                │
└─────────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────────┐
│         API GATEWAY — JWT Auth │ Rate Limit │ X-Tenant-Id            │
└─────────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────────┐
│          EVENT COLLECTOR SERVICE (Spring Boot WebFlux)               │
│  Avro validation → Redis SETNX dedup → INCR counter → Kafka publish │
└─────────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────────┐
│                 KAFKA (MSK) + SCHEMA REGISTRY                        │
│  Topics: {tenantId}.ad.events  │  3 brokers │ RF=3 │ 25 partitions  │
│  Partitioned by userId — guarantees per-user ordering for attribution│
└─────────────────────────────────────────────────────────────────────┘
           ↓ stateless routing         ↓ stateful CEP
┌──────────────────────┐   ┌────────────────────────────────────────┐
│       KsqlDB         │   │           Apache Flink                  │
│  Route by event type │   │  Session windows (30-min gap)           │
│  Filter invalids     │   │  Click-to-basket attribution            │
│  SQL — no deployment │   │  Exactly-once via checkpoint 2PC        │
│  for rule changes    │   │  Updates Redis attribution counters     │
└──────────────────────┘   └────────────────────────────────────────┘
           ↓                           ↓
┌─────────────────────────────────────────────────────────────────────┐
│                         STORAGE LAYER                                │
│                                                                      │
│  Redis (0–1h)          Druid (1h–90d)          Iceberg (raw/audit)  │
│  Real-time counters    Pre-aggregated OLAP      ACID, time travel    │
│  Atomic INCR           Native Kafka ingestion   Schema evolution     │
│  Sub-ms response       Datasource per tenant    Namespace per tenant │
│  Session dedup         Sub-second queries       S3 lifecycle 7yr     │
│                        Rollup at ingestion                           │
│                                                                      │
│  PostgreSQL (metadata: campaigns, tenants, attribution rules)        │
└─────────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────────┐
│              INSIGHTS API (Spring Boot WebFlux)                      │
│  GET /api/v1/ad/{campaignId}/clicks                                  │
│  GET /api/v1/ad/{campaignId}/impressions                             │
│  GET /api/v1/ad/{campaignId}/clickToBasket                           │
│  GET /api/v1/ad/{campaignId}/performance                             │
│  Real-time (no params) → Redis <1ms                                  │
│  Historical (from/to)  → Druid <100ms                                │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Technology Choices & Rationale

| Component | Technology | Why Chosen | Why NOT alternatives |
|-----------|------------|------------|---------------------|
| Event backbone | **Kafka (MSK)** | Replay critical — Druid outage = catch up on recovery. Partition by userId for session ordering. Vendor-neutral, runs anywhere. | Kinesis (AWS-only). RabbitMQ (no replay, deleted after consume). |
| Schema enforcement | **Schema Registry (Avro)** | Backward/forward compatibility enforced. Bad events rejected at ingestion, not discovered downstream. | JSON (no enforcement). |
| Stateless routing | **KsqlDB** | SQL routing rules — ops changes rules without developer deployment. Stateless = no savepoints. | Custom consumer (code deployment for rule changes). |
| Stateful processing | **Apache Flink** | True streaming (not micro-batch). Session windows for attribution. Savepoints for zero-downtime upgrades. Sub-100ms latency. | Spark Streaming (500ms min micro-batch — too slow for attribution). |
| Real-time store | **Redis Cluster** | Atomic INCR — sub-millisecond, no race conditions with concurrent pods. Auto-TTL. | DynamoDB (20ms). Memcached (no persistence). |
| Analytics store | **Apache Druid** | Pre-aggregation at ingestion (rollup) — 72M raw events → 7M aggregated. Native Kafka ingestion. Datasource-per-tenant isolation. **Production proven: achieved 8s→80ms improvement at IOT83.** | ClickHouse (evaluated — simpler ops, SQL-native, but Druid's rollup + Kafka-native better for this time-series IoT/ad use case). Elasticsearch (not OLAP). |
| Data lake | **Apache Iceberg** | ACID for concurrent Flink + Spark writes. Time travel for audits. Schema evolution without rewrites. Namespace-per-tenant. | Raw Parquet (no ACID — concurrent writes corrupt). Delta Lake (Spark-first, less Flink native support). |
| Metadata | **PostgreSQL** | ACID, relational, familiar. Campaign configs, attribution rules, tenant settings. | MongoDB (no joins for relational data). |
| API layer | **Spring Boot 3 + WebFlux** | Reactive — handles 10K concurrent dashboard requests without proportional threads. Non-blocking Redis + Druid. Production expertise. | Spring MVC (thread per request — 10K threads = 10GB RAM just for stacks). |
| Deployment | **AWS EKS + Helm + ArgoCD** | GitOps — rollback = git revert = 2 minutes. HPA on Kafka consumer lag (not CPU). | EC2 manual ops. |

> **On Druid vs ClickHouse:** I evaluated both. ClickHouse is simpler to operate and SQL-native.
> I chose Druid because I have run it in production at IOT83 at Fortune 500 scale — I know
> how to tune it, fix it at 3AM, and the rollup pre-aggregation is exactly what high-volume
> ad event analytics needs. That production confidence is worth more than theoretical simplicity.

---

## 3. Event Schema

```json
{
  "eventId":     "550e8400-e29b-41d4-a716-446655440000",
  "eventType":   "CLICK",
  "tenantId":    "tesco_uk",
  "campaignId":  "campaign_summer_2025",
  "adId":        "ad_sponsored_456",
  "userId":      "sha256_hashed_not_raw_pii",
  "sessionId":   "sess_789",
  "productId":   "prod_milk_2litre",
  "placementId": "search_results_slot_1",
  "timestamp":   1717123456789,
  "metadata": {
    "deviceType": "mobile",
    "pageUrl":    "/search?q=milk",
    "referrer":   "search_results"
  }
}
```

**Event types:** `IMPRESSION` `CLICK` `PRODUCT_VIEW` `ADD_TO_CART` `PURCHASE`

**Critical:** `timestamp` = event time from client, NOT server arrival time.
Mobile events can arrive minutes late when user reconnects.
Flink uses event time for correct attribution window calculation.

**Privacy:** `userId` is always `SHA-256(loyaltyId + salt)` — never raw PII.

---

## 4. Data Storage Design

### Why four stores, not one

| Store | Data | Latency | Why not just one store |
|-------|------|---------|----------------------|
| Redis | Real-time counters (0–1h) | <1ms | Druid has 30–60s ingestion lag — too slow for live dashboard |
| Druid | Pre-aggregated analytics (1h–90d) | <100ms | Redis too expensive for 90 days of data in memory |
| Iceberg | Raw events (unlimited) | Seconds | Druid pre-aggregation loses raw event fidelity |
| PostgreSQL | Metadata | <10ms | Not appropriate in event stores |

### Druid table schema (per tenant datasource)

```sql
-- Druid ingestion spec creates this via native Kafka ingestion
-- Pre-aggregated at ingestion time (rollup)
-- tenantId_ad_events datasource per tenant

__time          TIMESTAMP  (hourly granularity)
campaign_id     STRING
impression_count LONG      (SUM at ingestion = rollup)
click_count      LONG      (SUM at ingestion = rollup)
click_to_basket_count LONG (from Flink attribution job)
unique_users     LONG      (HyperLogLog approximate count)
```

### Data Retention Strategy

| Store | Retention | Strategy |
|-------|-----------|----------|
| Redis | 25 hours | Auto-TTL, no cleanup needed |
| Druid | 90 days | Auto-drop segments after 90 days |
| Iceberg S3 Standard | 0–30 days | Hot raw events |
| Iceberg S3 IA | 30–365 days | Warm archive, 40% cheaper |
| Iceberg S3 Glacier | 1–7 years | Regulatory compliance |
| PostgreSQL | Permanent | Campaigns/tenants |

---

## 5. Insights API

### Endpoints

```
GET /api/v1/ad/{campaignId}/clicks
GET /api/v1/ad/{campaignId}/impressions
GET /api/v1/ad/{campaignId}/clickToBasket
GET /api/v1/ad/{campaignId}/performance
```

### Routing Strategy

```
No time params → Redis   (real-time, <1ms, rolling 1h)
from + to      → Druid   (historical, <100ms, up to 90 days)
```

### Sample Responses

```json
GET /api/v1/ad/campaign_summer_2025/clicks
Headers: X-Tenant-Id: tesco_uk

{
  "metric": "clicks",
  "value": 1500,
  "campaignId": "campaign_summer_2025",
  "source": "realtime-cache",
  "timestamp": "2025-06-01T10:30:00"
}

GET /api/v1/ad/campaign_summer_2025/performance
{
  "metric": "performance",
  "value": {
    "campaignId": "campaign_summer_2025",
    "impressions": 50000,
    "clicks": 1500,
    "clickToBasket": 300,
    "ctr": 3.0,
    "conversionRate": 20.0
  },
  "source": "realtime-cache"
}
```

---

## 6. Scalability Strategy

### Horizontal Scaling

**Kafka (MSK):** Absorbs all bursts. Producers never blocked. Consumers catch up.

**HPA on consumer lag (NOT CPU):**
Consumer can be at 0% CPU while falling behind on messages.
CPU-based HPA would never fire. Lag = correct signal for streaming.
`maxReplicas = partition count (25)` — hard rule. Extra consumers beyond
partition count receive zero messages and idle.

**Load Balancing Layers:**
```
Route 53     → geographic routing (UK retailers → eu-west-1)
ALB          → path routing (/events → collector, /ad → API)
K8s Service  → round-robin across stateless pods
Kafka        → partition by userId (per-user ordering for attribution)
```

### Traffic Spike (Black Friday, Prime Day)

```
1. Pre-scale 2h before known event
   Cron-triggered HPA override

2. Kafka absorbs burst
   Even if API pods overwhelmed,
   events safely queued in Kafka

3. HPA reacts within 3 minutes
   (15s scrape + 60s cooldown + 60s pod start)

4. Tenant-level rate limits enforced at Gateway
   Enterprise: 100K events/sec
   Standard:    10K events/sec
   Basic:         1K events/sec
```

---

## 7. Multi-Tenancy

### Six Isolation Layers

```
1. JWT         tenantId as signed Keycloak claim — tamper-proof
2. Headers     X-Tenant-Id on every downstream request
3. Kafka       {tenantId}.ad.events — physical topic separation
4. Redis       {tenantId}:campaign:{id}:clicks — key prefix
5. Druid       {tenantId}_ad_events — datasource per tenant
6. Iceberg     iceberg.{tenantId}.ad_events — namespace per tenant

Rule: No query runs without tenantId filter.
      No findAll() anywhere in codebase.
      DB user for tenant A has no permissions on tenant B's tables.
```

### Tenant Tier Model

| Feature | Enterprise | Standard | Basic |
|---------|-----------|----------|-------|
| CPU quota | 40 cores | 4 cores | 1 core |
| Kafka partitions | 50 dedicated | 10 shared | 5 shared |
| Flink job | Dedicated | Shared | Shared |
| Druid | Dedicated datasource | Shared | Shared |
| Max events/sec | 100,000 | 10,000 | 1,000 |
| SLA | 99.99% | 99.9% | 99.5% |

---

## 8. Deployment Overview

```
AWS eu-west-1 (UK/EU retailers)

AWS Managed:
  MSK (Kafka) — 3 brokers, 3 AZs, auto-scaling storage
  ElastiCache (Redis) — Multi-AZ, auto-failover
  RDS PostgreSQL — Multi-AZ
  S3 (Iceberg) — lifecycle policies
  ALB — SSL, WAF, path routing

EKS Cluster:
  namespace: ingestion
    event-collector (HPA: 3–20 pods, CPU trigger)
    schema-registry (3 pods, StatefulSet)

  namespace: streaming
    flink-jobmanager (1 pod, leader-elected)
    flink-taskmanagers (HPA: Flink backpressure)
    ksqldb (3 pods, StatefulSet)

  namespace: api
    insights-api (HPA: 3–30 pods, CPU trigger)

  namespace: data
    druid (StatefulSet: coordinator, broker, historical, middlemanager)
    spark-operator (batch compaction, Airflow-scheduled)

  namespace: monitoring
    prometheus + alertmanager
    grafana + loki + tempo + pyroscope

  namespace: tenant-{id} (Enterprise only)
    dedicated flink job
    ResourceQuota: CPU/memory enforced

CI/CD:
  GitHub → Jenkins (9 stages) → ArgoCD → K8s rolling update
  Rollback = git revert = 2 minutes
```

---

## 9. Metrics and Monitoring

### Key Metrics

| Metric | Alert Threshold | Why Critical |
|--------|----------------|--------------|
| `kafka_consumer_group_lag` | >10,000 per pod | Events backing up — HPA trigger |
| `api_response_time_p99` | >500ms | SLA breach for marketer dashboard |
| `redis_hit_rate` | <90% | Cache not effective |
| `druid_query_time_p99` | >1000ms | Analytics DB degraded |
| `flink_attribution_lag` | >30s | Attribution job falling behind |
| `kafka_under_replicated_partitions` | >0 | Broker health issue |
| `event_ingestion_error_rate` | >1% | Schema or validation issue |

### Tools

```
Prometheus   → scrapes all pods every 15s via /actuator/prometheus
Grafana      → dashboards + visual alerts
Loki         → structured JSON logs; MDC: traceId + tenantId on every line
Tempo        → distributed tracing end-to-end
Pyroscope    → continuous profiling (found 2AM memory leak at IOT83)
PagerDuty    → on-call escalation
```

---

## 10. Privacy and GDPR

**Critical for dunnhumby — UK-based, powers Tesco loyalty data**

```
1. User ID hashing
   userId = SHA-256(loyaltyCardId + salt)
   Never raw PII in event pipeline

2. Consent check
   Event Collector checks consent store
   before publishing to Kafka
   Non-consented events dropped silently

3. Right to erasure (GDPR Article 17)
   Redis: DEL all user keys
   Druid: DROP SEGMENT for affected periods
   Iceberg: new snapshot excludes user
   SLA: 30 days

4. Data residency
   UK tenant data: eu-west-2 (London)
   EU tenant data: eu-west-1 (Ireland)
   Configured per tenant in tenant_configs

5. First-party data advantage
   dunnhumby's USP: loyalty card purchase history
   No third-party cookies needed
   GDPR compliant — users consent to loyalty programme
   Cookie deprecation irrelevant for this platform
```

---

## 11. Cost Analysis

### Monthly estimate (10M events/day, 5 tenants)

| Component | Service | Cost/month |
|-----------|---------|------------|
| Kafka | MSK 3x broker.kafka.m5.large | $450 |
| Kafka storage | 2TB | $100 |
| Redis | ElastiCache 2x cache.r6g.large | $280 |
| Druid | EKS 4 nodes m5.xlarge + 5TB EBS | $920 |
| Iceberg | S3 Standard + IA 10TB | $250 |
| Flink | EKS nodes shared | $300 |
| API pods | EKS 3–10 pods avg | $200 |
| PostgreSQL | RDS db.t3.medium Multi-AZ | $180 |
| Monitoring | EKS Prometheus/Grafana/Loki | $150 |
| ALB + Route53 | Fixed | $80 |
| **TOTAL** | | **~$2,910/month** |

### Cost Optimisations

```
1. Spot instances for Flink/Spark batch  → saves ~$150/month
   Flink checkpoints = safe on spot termination

2. Druid segment auto-drop after 90 days → saves ~$200/month
   Keeps only what is needed

3. Iceberg compaction (Spark hourly)     → saves ~$80/month
   60K small files → 50 files = 10x fewer S3 GET calls

4. Kafka tiered storage                  → saves ~$60/month
   Old segments to S3 automatically

5. HPA scale-down off-peak               → saves ~$150/month

OPTIMISED: ~$2,270/month = ~$27,000/year
```

### Cost Monitoring

```
Every K8s resource tagged: tenantId, service, environment
AWS Cost Explorer per-tenant cost report monthly
Alert: CloudWatch billing >80% of monthly budget
Chargeback model: $X per million events ingested
```

---

## 12. Challenges and Trade-offs

### Real-time accuracy vs latency
Redis counters are approximate under extreme concurrency.
INCR is atomic but 20 pods incrementing simultaneously means
two rapid API reads could see slightly different values.
**Decision:** Acceptable for live dashboards. Billing uses Druid
(Kafka exactly-once ingestion = always accurate, 60s delay).
Rule: "Live data approximate to the second. Reports exact to the event."

### Click-to-basket attribution window
30-minute window may attribute unrelated cart additions.
User clicks ad, adds to cart 28 minutes later after visiting 10 pages.
**Decision:** Industry standard last-click attribution within window.
Window is tenant-configurable (5min impulse, 24h considered purchase).
Expose raw click and cart data for custom attribution in Druid.

### Late-arriving events
Mobile events arrive late when user reconnects from offline.
**Decision:** Flink event-time processing with 10-minute allowed lateness.
Events after 10 minutes → side output → reconciliation job.
Dashboard shows note: "may include late events."

### Kafka partition count lock-in
Cannot reduce partitions. Increasing breaks key routing.
**Decision:** Start with 25 per tenant at onboarding (supports 25 consumers max).
If tenant grows beyond this, create new topic with more partitions and migrate
via planned maintenance window — not an emergency.

### Multi-tenancy vs cost
Dedicated resources = better isolation, higher cost.
Shared = lower cost, noisy-neighbour risk.
**Decision:** Tier model. Enterprise = dedicated. Standard/Basic = shared with
ResourceQuota enforcement. One tenant's Black Friday cannot starve another.

---

## 13. Local Setup

```bash
# Start all infrastructure
docker-compose up -d

# Wait for Kafka ready
sleep 30

# Run the application
./gradlew bootRun

# Ingest a test click event
curl -X POST http://localhost:8080/api/v1/events \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Id: retailer_test" \
  -d '{
    "eventId":   "test-001",
    "eventType": "CLICK",
    "campaignId":"campaign_summer_2025",
    "userId":    "user_hash_abc123",
    "timestamp": 1717123456789
  }'

# Query real-time clicks
curl -H "X-Tenant-Id: retailer_test" \
  http://localhost:8080/api/v1/ad/campaign_summer_2025/clicks

# Query historical (Druid)
curl -H "X-Tenant-Id: retailer_test" \
  "http://localhost:8080/api/v1/ad/campaign_summer_2025/clicks?from=2025-06-01T00:00:00&to=2025-06-30T23:59:59"

# Full performance
curl -H "X-Tenant-Id: retailer_test" \
  http://localhost:8080/api/v1/ad/campaign_summer_2025/performance
```

---

## Project Structure

```
src/main/java/com/retailmedia/insights/
├── controller/
│   └── CampaignInsightsController.java    # 4 REST endpoints
├── service/
│   ├── CampaignInsightsService.java       # Redis vs Druid routing
│   └── EventCollectorService.java         # Ingest + dedup + Kafka
├── repository/
│   ├── RedisMetricsRepository.java        # Real-time counters
│   └── DruidMetricsRepository.java        # Historical queries
├── dto/
│   ├── MetricResponseDTO.java
│   └── CampaignMetricsDTO.java
├── event/
│   └── AdEvent.java                       # Event model
├── exception/
│   ├── CampaignNotFoundException.java
│   └── GlobalExceptionHandler.java
└── config/
    ├── RedisConfig.java
    └── DruidConfig.java
src/test/
├── controller/CampaignInsightsControllerTest.java
└── service/CampaignInsightsServiceTest.java
docker-compose.yml
docker/druid-init/
docker/postgres-init.sql
```
