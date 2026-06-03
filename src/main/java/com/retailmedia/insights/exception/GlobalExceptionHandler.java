package com.retailmedia.insights.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.MissingRequestValueException;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Consistent error responses across all endpoints.
 * Never exposes stack traces or internal details to clients.
 */
@RestControllerAdvice @Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(CampaignNotFoundException.class)
    public Mono<ResponseEntity<Map<String, Object>>> notFound(CampaignNotFoundException e) {
        return Mono.just(ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(error("CAMPAIGN_NOT_FOUND", e.getMessage())));
    }

    @ExceptionHandler(WebExchangeBindException.class)
    public Mono<ResponseEntity<Map<String, Object>>> validation(WebExchangeBindException e) {
        String msg = e.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .findFirst().orElse("Validation failed");
        return Mono.just(ResponseEntity.badRequest().body(error("VALIDATION_ERROR", msg)));
    }

    @ExceptionHandler(MissingRequestValueException.class)
    public Mono<ResponseEntity<Map<String, Object>>> missingRequestValue(MissingRequestValueException e) {
        return Mono.just(ResponseEntity.badRequest().body(error("BAD_REQUEST", e.getReason())));
    }

    @ExceptionHandler(Exception.class)
    public Mono<ResponseEntity<Map<String, Object>>> general(Exception e) {
        log.error("Unhandled exception", e);
        return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(error("INTERNAL_ERROR", "An unexpected error occurred")));
    }

    private Map<String, Object> error(String code, String message) {
        return Map.of("error", code, "message", message, "timestamp", LocalDateTime.now().toString());
    }
}
