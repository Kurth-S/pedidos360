package cl.duoc.pedidos360.messaging.event;

/** Evento "un producto pedido tiene poco stock". Lo publica ms-productos; ms-pedidos alerta al administrador. */
public record StockBajoEvent(
    Long productoId,
    String nombreProducto,
    Integer stockActual,
    Integer umbral,
    Long pedidoId) {}
