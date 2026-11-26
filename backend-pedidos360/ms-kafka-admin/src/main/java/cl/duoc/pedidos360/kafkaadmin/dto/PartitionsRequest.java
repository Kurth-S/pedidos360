package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/** Nuevo número total de particiones (Kafka solo permite aumentarlas). */
public record PartitionsRequest(@NotNull @Min(2) @Max(100) Integer partitions) {}
