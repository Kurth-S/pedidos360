package cl.duoc.pedidos360.messaging.inventario;

import cl.duoc.pedidos360.messaging.config.InventarioProperties;
import cl.duoc.pedidos360.messaging.event.PedidoCreadoEvent;
import cl.duoc.pedidos360.messaging.event.StockBajoEvent;
import cl.duoc.pedidos360.messaging.producer.StockEventPublisher;
import cl.duoc.pedidos360.messaging.support.ConfirmacionMensajes;
import cl.duoc.pedidos360.messaging.support.MensajeInvalidoException;
import cl.duoc.pedidos360.productos.model.Producto;
import cl.duoc.pedidos360.productos.repository.ProductoRepository;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Dominio "inventario": cuando se crea un pedido, revisa el stock de cada producto pedido y,
 * si está bajo el umbral, publica una alerta. No modifica el stock (la lógica existente no cambia).
 */
@Component
public class InventarioPedidoConsumer {

  private static final Logger log = LoggerFactory.getLogger(InventarioPedidoConsumer.class);

  private final ConfirmacionMensajes confirmacion;
  private final ProductoRepository productos;
  private final StockEventPublisher publisher;
  private final InventarioProperties inventario;

  public InventarioPedidoConsumer(ConfirmacionMensajes confirmacion, ProductoRepository productos,
                                  StockEventPublisher publisher, InventarioProperties inventario) {
    this.confirmacion = confirmacion;
    this.productos = productos;
    this.publisher = publisher;
    this.inventario = inventario;
  }

  @RabbitListener(queues = "${pedidos360.rabbit.queues.inventario.name}")
  public void alCrearsePedido(PedidoCreadoEvent evento,
                              Channel channel,
                              @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                              @Header(name = AmqpHeaders.REDELIVERED, defaultValue = "false") boolean redelivered) {
    confirmacion.procesar("inventario", evento, channel, tag, redelivered, () -> {
      if (evento.pedidoId() == null || evento.items() == null || evento.items().isEmpty()) {
        throw new MensajeInvalidoException("El pedido no trae id o ítems para revisar el inventario");
      }
      for (PedidoCreadoEvent.Item item : evento.items()) {
        Producto producto = productos.findById(item.productoId()).orElse(null);
        if (producto == null) {
          log.warn("[INVENTARIO] El producto {} del pedido {} ya no existe; se omite", item.productoId(), evento.pedidoId());
          continue;
        }
        if (producto.getStock() <= inventario.umbralStockBajo()) {
          publisher.publicarStockBajo(new StockBajoEvent(producto.getId(), producto.getNombre(),
              producto.getStock(), inventario.umbralStockBajo(), evento.pedidoId()));
        }
      }
      log.info("[INVENTARIO] Revisión de stock del pedido {} terminada", evento.pedidoId());
    });
  }
}
