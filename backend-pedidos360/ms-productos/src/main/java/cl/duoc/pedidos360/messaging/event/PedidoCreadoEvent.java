package cl.duoc.pedidos360.messaging.event;

import java.util.List;

/**
 * Vista que ms-productos necesita del evento "pedido creado" (publicado por ms-pedidos).
 * Los demás campos del mensaje se ignoran.
 */
public record PedidoCreadoEvent(Long pedidoId, List<Item> items) {

  public record Item(Long productoId, String nombreProducto, Integer cantidad) {}
}
