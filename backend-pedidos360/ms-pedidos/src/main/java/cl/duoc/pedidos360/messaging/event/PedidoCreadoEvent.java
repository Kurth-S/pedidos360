package cl.duoc.pedidos360.messaging.event;

import cl.duoc.pedidos360.pedidos.model.Pedido;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** Evento "se creó un pedido". Lo publica ms-pedidos y lo consumen notificaciones, tickets e inventario. */
public record PedidoCreadoEvent(
    Long pedidoId,
    String clienteOid,
    String clienteEmail,
    String clienteNombre,
    BigDecimal total,
    Instant fecha,
    List<Item> items) {

  public record Item(Long productoId, String nombreProducto, Integer cantidad, BigDecimal subtotal) {}

  public static PedidoCreadoEvent de(Pedido p) {
    List<Item> items = p.getItems().stream()
        .map(i -> new Item(i.getProductoId(), i.getNombreProducto(), i.getCantidad(), i.getSubtotal()))
        .toList();
    return new PedidoCreadoEvent(p.getId(), p.getClienteOid(), p.getClienteEmail(), p.getClienteNombre(),
        p.getTotal(), p.getFecha(), items);
  }
}
