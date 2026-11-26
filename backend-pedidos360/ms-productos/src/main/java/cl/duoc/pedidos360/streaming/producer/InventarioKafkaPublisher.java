package cl.duoc.pedidos360.streaming.producer;

import cl.duoc.pedidos360.streaming.config.KafkaSettings;
import cl.duoc.pedidos360.streaming.event.InventarioEventoDto;
import cl.duoc.pedidos360.streaming.support.KafkaPublicacionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Productor del tópico {@code pedidos360.inventario.eventos}. Se invoca desde un consumidor, así que espera la
 * confirmación del broker: si falla, el error sube y el consumidor reintenta (y al final va a la DLT).
 */
@Component
public class InventarioKafkaPublisher {

  private static final Logger log = LoggerFactory.getLogger(InventarioKafkaPublisher.class);
  private static final long CONFIRMACION_SEGUNDOS = 10;

  private final KafkaTemplate<String, InventarioEventoDto> kafkaTemplate;
  private final KafkaSettings settings;

  public InventarioKafkaPublisher(KafkaTemplate<String, InventarioEventoDto> kafkaTemplate, KafkaSettings settings) {
    this.kafkaTemplate = kafkaTemplate;
    this.settings = settings;
  }

  public void publicar(InventarioEventoDto evento) {
    String topico = settings.topics().inventarioEventos();
    try {
      SendResult<String, InventarioEventoDto> resultado = kafkaTemplate
          .send(topico, String.valueOf(evento.productoId()), evento)
          .get(CONFIRMACION_SEGUNDOS, TimeUnit.SECONDS);
      RecordMetadata meta = resultado.getRecordMetadata();
      log.info("[KAFKA] {} del producto {} publicado en {} (partición {}, offset {}, stock {}, bajo={})",
          evento.tipo(), evento.productoId(), topico, meta.partition(), meta.offset(), evento.stockActual(), evento.stockBajo());
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new KafkaPublicacionException("Publicación interrumpida en '" + topico + "'", ex);
    } catch (ExecutionException | TimeoutException ex) {
      throw new KafkaPublicacionException("No se pudo publicar el evento del producto " + evento.productoId()
          + " en '" + topico + "': " + ex.getMessage(), ex);
    }
  }
}
