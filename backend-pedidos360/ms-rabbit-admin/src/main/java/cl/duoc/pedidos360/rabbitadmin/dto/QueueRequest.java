package cl.duoc.pedidos360.rabbitadmin.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * Datos para crear una cola.
 *
 * @param name nombre (letras, números, '.', '_' y '-')
 * @param durable si sobrevive al reinicio del broker (por defecto true)
 * @param deadLetterExchange exchange al que van los mensajes rechazados (opcional)
 * @param deadLetterRoutingKey routing key con la que se reenvían a la DLQ (opcional; requiere deadLetterExchange)
 * @param messageTtlMs vida máxima de un mensaje en la cola, en milisegundos (opcional)
 */
public record QueueRequest(
    @NotBlank(message = "el nombre de la cola es obligatorio")
    @Size(max = 255, message = "el nombre no puede superar 255 caracteres")
    @Pattern(regexp = "^[A-Za-z0-9._-]+$", message = "solo letras, números, '.', '_' y '-'")
    String name,
    Boolean durable,
    @Size(max = 255, message = "el exchange de DLQ no puede superar 255 caracteres")
    @Pattern(regexp = "^[A-Za-z0-9._-]*$", message = "solo letras, números, '.', '_' y '-'")
    String deadLetterExchange,
    @Size(max = 255, message = "la routing key de DLQ no puede superar 255 caracteres")
    String deadLetterRoutingKey,
    @Min(value = 1, message = "el TTL debe ser mayor a 0 ms")
    Long messageTtlMs) {

  public QueueRequest {
    if (durable == null) {
      durable = Boolean.TRUE;
    }
  }
}
