package cl.duoc.pedidos360.streaming.event;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** Evento del tópico {@code pedidos360.pedidos.eventos} (lo publica ms-pedidos). Mismo contrato JSON en todos los servicios. */
public record PedidoEventoDto(
    @NotBlank String eventId,
    @NotBlank String tipo,
    @NotNull Instant ocurridoEn,
    @NotNull Long pedidoId,
    @NotBlank String clienteOid,
    @NotNull @PositiveOrZero BigDecimal total,
    @NotEmpty List<@Valid Item> items) {

  public static final String PEDIDO_CREADO = "PEDIDO_CREADO";

  public record Item(
      @NotNull Long productoId,
      @NotBlank String nombreProducto,
      @NotNull @Positive Integer cantidad,
      @NotNull @PositiveOrZero BigDecimal subtotal) {}
}
