package com.rzodeczko.presentation.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.LocalDateTime;
import java.util.Map;

public record ErrorResponseDto(
        String message,
        LocalDateTime timestamp,
        @JsonInclude(JsonInclude.Include.NON_NULL)
        Map<String, String> validationErrors
) {
    public ErrorResponseDto(String message) {
        this(message, LocalDateTime.now(), null);
    }

    public ErrorResponseDto(String message, Map<String, String> validationErrors) {
        this(message, LocalDateTime.now(), validationErrors);
    }
}
