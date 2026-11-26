package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.Map;

/** Solicitud de creación de un tópico. */
public record TopicRequest(
    @NotBlank @Size(max = 249)
    @Pattern(regexp = "[A-Za-z0-9._-]+", message = "solo letras, números, '.', '_' y '-'")
    String name,
    @NotNull @Min(1) @Max(100) Integer partitions,
    @NotNull @Min(1) @Max(5) Integer replicationFactor,
    Map<String, String> configs) {}
