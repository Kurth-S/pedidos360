package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/** Reinicio de offsets de un consumer group sobre un tópico (el grupo debe estar sin consumidores activos). */
public record ResetOffsetsRequest(@NotBlank String topic, @NotNull Position to) {

  public enum Position { EARLIEST, LATEST }
}
