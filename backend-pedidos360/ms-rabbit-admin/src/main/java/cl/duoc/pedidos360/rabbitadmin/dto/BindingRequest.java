package cl.duoc.pedidos360.rabbitadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/** Enlace entre un exchange y una cola mediante una routing key (puede ser vacía en exchanges fanout). */
public record BindingRequest(
    @NotBlank(message = "la cola es obligatoria") String queue,
    @NotBlank(message = "el exchange es obligatorio") String exchange,
    @NotNull(message = "la routingKey es obligatoria (puede ser vacía)")
    @Size(max = 255, message = "la routingKey no puede superar 255 caracteres")
    String routingKey) {}
