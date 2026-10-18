package cl.duoc.pedidos360.rabbitadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/** Datos para crear un exchange. */
public record ExchangeRequest(
    @NotBlank(message = "el nombre del exchange es obligatorio")
    @Size(max = 255, message = "el nombre no puede superar 255 caracteres")
    @Pattern(regexp = "^[A-Za-z0-9._-]+$", message = "solo letras, números, '.', '_' y '-'")
    String name,
    @NotNull(message = "el tipo es obligatorio (DIRECT, TOPIC, FANOUT o HEADERS)")
    ExchangeType type,
    Boolean durable) {

  public ExchangeRequest {
    if (durable == null) {
      durable = Boolean.TRUE;
    }
  }
}
