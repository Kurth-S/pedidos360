package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.NotBlank;

/** Mensaje de prueba: se publica tal cual (texto), por lo que puede ser un JSON válido o uno roto. */
public record PublishRequest(@NotBlank String topic, String key, @NotBlank String payload) {}
