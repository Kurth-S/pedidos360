package cl.duoc.pedidos360.streaming.consumer;

import cl.duoc.pedidos360.messaging.config.InventarioProperties;
import cl.duoc.pedidos360.productos.model.Producto;
import cl.duoc.pedidos360.productos.repository.ProductoRepository;
import cl.duoc.pedidos360.streaming.event.InventarioEventoDto;
import cl.duoc.pedidos360.streaming.event.PedidoEventoDto;
import cl.duoc.pedidos360.streaming.producer.InventarioKafkaPublisher;
import cl.duoc.pedidos360.streaming.support.ValidadorEventos;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor del tópico de pedidos (consumer group {@code productos.inventario-demanda}).
 *
 * <p>Por cada pedido creado lee el stock vigente de cada producto y publica su estado en el tópico de inventario:
 * es un pequeño proceso de streaming (consume un tópico y produce en otro). No toca el stock ni la lógica de
 * RabbitMQ: la alerta de stock bajo por RabbitMQ sigue funcionando exactamente igual.
 */
@Component
public class PedidoKafkaConsumer {

  private static final Logger log = LoggerFactory.getLogger(PedidoKafkaConsumer.class);

  private final ValidadorEventos validador;
  private final ProductoRepository productos;
  private final InventarioProperties inventario;
  private final InventarioKafkaPublisher publisher;

  public PedidoKafkaConsumer(ValidadorEventos validador, ProductoRepository productos,
                             InventarioProperties inventario, InventarioKafkaPublisher publisher) {
    this.validador = validador;
    this.productos = productos;
    this.inventario = inventario;
    this.publisher = publisher;
  }

  @KafkaListener(
      topics = "${pedidos360.kafka.topics.pedidos-eventos}",
      groupId = "${pedidos360.kafka.groups.inventario-demanda}",
      containerFactory = "pedidosKafkaListenerFactory")
  public void alRecibirPedido(ConsumerRecord<String, PedidoEventoDto> registro) {
    PedidoEventoDto evento = validador.validar(registro.value(), "Evento de pedido");

    if (!PedidoEventoDto.PEDIDO_CREADO.equals(evento.tipo())) {
      log.debug("[KAFKA][INVENTARIO] Tipo '{}' ignorado (pedido {})", evento.tipo(), evento.pedidoId());
      return;
    }
    log.info("[KAFKA][INVENTARIO] Pedido {} recibido (partición {}, offset {}, {} ítems)",
        evento.pedidoId(), registro.partition(), registro.offset(), evento.items().size());

    for (PedidoEventoDto.Item item : evento.items()) {
      Producto producto = productos.findById(item.productoId()).orElse(null);
      if (producto == null) {
        log.warn("[KAFKA][INVENTARIO] El producto {} del pedido {} ya no existe; se omite", item.productoId(), evento.pedidoId());
        continue;
      }
      int umbral = inventario.umbralStockBajo();
      // eventId determinista: si el mensaje se reintenta, el consumidor aguas abajo reconoce el duplicado
      String eventId = UUID.nameUUIDFromBytes((evento.pedidoId() + ":" + producto.getId()).getBytes()).toString();
      publisher.publicar(new InventarioEventoDto(eventId, InventarioEventoDto.STOCK_REVISADO, Instant.now(),
          producto.getId(), producto.getNombre(), producto.getStock(), umbral,
          producto.getStock() <= umbral, evento.pedidoId(), item.cantidad()));
    }
  }
}
