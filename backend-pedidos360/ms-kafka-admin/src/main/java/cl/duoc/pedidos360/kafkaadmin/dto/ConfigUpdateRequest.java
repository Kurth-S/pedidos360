package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.NotEmpty;
import java.util.Map;

/** Configuraciones del tópico que se desean fijar (retention.ms, cleanup.policy, ...). */
public record ConfigUpdateRequest(@NotEmpty Map<String, String> configs) {}
