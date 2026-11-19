package org.studentdiscounts.moderation;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("moderation")
public record ModerationProperties(
    @Min(1024) @Max(1048576) int maxBodyBytes,
    @Min(1) @Max(10000) int requestsPerMinute,
    boolean outboxEnabled,
    boolean listenerEnabled,
    boolean localTopicsEnabled,
    @Min(1) @Max(60000) long sendTimeoutMs) {}
