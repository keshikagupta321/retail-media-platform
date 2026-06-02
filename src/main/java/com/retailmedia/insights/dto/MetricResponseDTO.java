package com.retailmedia.insights.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;
import java.time.LocalDateTime;

@Data @Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MetricResponseDTO<T> {
    private String metric;
    private T value;
    private String campaignId;
    private LocalDateTime from;
    private LocalDateTime to;
    /** "realtime-cache" (Redis) or "historical-druid" — shows client data freshness */
    private String source;
    private LocalDateTime timestamp;

    public static <T> MetricResponseDTO<T> of(String metric, T value, String campaignId,
                                               LocalDateTime from, LocalDateTime to) {
        return MetricResponseDTO.<T>builder()
                .metric(metric).value(value).campaignId(campaignId)
                .from(from).to(to)
                .source(from == null ? "realtime-cache" : "historical-druid")
                .timestamp(LocalDateTime.now())
                .build();
    }
}
