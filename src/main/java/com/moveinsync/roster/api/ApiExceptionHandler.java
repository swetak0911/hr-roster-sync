package com.moveinsync.roster.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log =
            LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<Map<String, Object>> validation(MethodArgumentNotValidException ex) {
        return ResponseEntity.badRequest().body(Map.of(
                "code", "VALIDATION_FAILED",
                "message", "One or more fields are invalid"
        ));
    }

    @ExceptionHandler(AccessDeniedException.class)
    ResponseEntity<Map<String, Object>> accessDenied(AccessDeniedException ex) {
        return ResponseEntity.status(403).body(Map.of(
                "code", "FORBIDDEN",
                "message", "Access is denied"
        ));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<Map<String, Object>> badRequest(IllegalArgumentException ex) {
        return ResponseEntity.status(404).body(Map.of(
                "code", ex.getMessage() != null ? ex.getMessage() : "NOT_FOUND",
                "message", "Requested resource was not found"
        ));
    }

    @ExceptionHandler(IllegalStateException.class)
    ResponseEntity<Map<String, Object>> conflict(IllegalStateException ex) {
        return ResponseEntity.status(409).body(Map.of(
                "code", ex.getMessage() != null ? ex.getMessage() : "CONFLICT",
                "message", "Requested state transition is not allowed"
        ));
    }

    @ExceptionHandler(ResponseStatusException.class)
    ResponseEntity<Map<String, Object>> status(ResponseStatusException ex) {
        return ResponseEntity.status(ex.getStatusCode()).body(Map.of(
                "code", "REQUEST_NOT_ALLOWED",
                "message", "Request is not allowed"
        ));
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<Map<String, Object>> internal(Exception ex) {
        log.error("Unhandled API exception: {} - {}", ex.getClass().getName(), ex.getMessage(), ex);

        return ResponseEntity.internalServerError().body(Map.of(
                "code", "INTERNAL_ERROR",
                "message", "Request could not be completed"
        ));
    }
}
