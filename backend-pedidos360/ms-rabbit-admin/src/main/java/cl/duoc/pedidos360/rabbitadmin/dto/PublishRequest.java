package cl.duoc.pedidos360.rabbitadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/** Mensaje JSON de prueba para publicar a mano (sirve para demostrar consumidores y DLQ). */
public record PublishRequest(
    @NotBlank(message = "el exchange es obligatorio") String exchange,
    @NotNull(message = "la routingKey es obligatoria (puede ser vacía)") String routingKey,
    @NotBlank(message = "el cuerpo JSON es obligatorio")
    @Size(max = 10000, message = "el cuerpo no puede superar 10000 caracteres")
    String payload) {}
