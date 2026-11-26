package cl.duoc.pedidos360.streaming.event;

import cl.duoc.pedidos360.pedidos.model.Pedido;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Evento de negocio del tópico {@code pedidos360.pedidos.eventos}. Un registro por hecho ocurrido.
 * La clave del mensaje es el id del pedido, así todos los eventos de un pedido quedan en la misma partición y en orden.
 */
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

  public static PedidoEventoDto creado(Pedido p) {
    List<Item> items = p.getItems().stream()
        .map(i -> new Item(i.getProductoId(), i.getNombreProducto(), i.getCantidad(), i.getSubtotal()))
        .toList();
    return new PedidoEventoDto(UUID.randomUUID().toString(), PEDIDO_CREADO,
        p.getFecha() != null ? p.getFecha() : Instant.now(),
        p.getId(), p.getClienteOid(), p.getTotal(), items);
  }
}
