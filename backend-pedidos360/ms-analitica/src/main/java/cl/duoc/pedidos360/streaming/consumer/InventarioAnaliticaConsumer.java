package cl.duoc.pedidos360.streaming.consumer;

import cl.duoc.pedidos360.analitica.AnaliticaStore;
import cl.duoc.pedidos360.streaming.event.InventarioEventoDto;
import cl.duoc.pedidos360.streaming.support.EventoInvalidoException;
import cl.duoc.pedidos360.streaming.support.ValidadorEventos;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.stereotype.Component;

/**
 * Consumer group {@code analitica.inventario}: lee el tópico de inventario y mantiene el último stock conocido de cada
 * producto y las alertas de stock bajo. Es un grupo distinto al de ventas: cada grupo recibe todos los mensajes de su tópico.
 */
@Component
public class InventarioAnaliticaConsumer extends AbstractConsumerSeekAware {

  private static final Logger log = LoggerFactory.getLogger(InventarioAnaliticaConsumer.class);

  private final ValidadorEventos validador;
  private final AnaliticaStore store;
  private final boolean reconstruirAlIniciar;

  public InventarioAnaliticaConsumer(ValidadorEventos validador, AnaliticaStore store,
                                     @Value("${pedidos360.analitica.reconstruir-al-iniciar:true}") boolean reconstruirAlIniciar) {
    this.validador = validador;
    this.store = store;
    this.reconstruirAlIniciar = reconstruirAlIniciar;
  }

  @KafkaListener(
      topics = "${pedidos360.kafka.topics.inventario-eventos}",
      groupId = "${pedidos360.kafka.groups.analitica-inventario}",
      containerFactory = "inventarioKafkaListenerFactory")
  public void alRecibirInventario(ConsumerRecord<String, InventarioEventoDto> registro) {
    InventarioEventoDto evento = validador.validar(registro.value(), "Evento de inventario");
    if (!InventarioEventoDto.STOCK_REVISADO.equals(evento.tipo())) {
      throw new EventoInvalidoException("Tipo de evento de inventario desconocido: " + evento.tipo());
    }
    boolean nuevo = store.registrarInventario(evento);
    log.info("[KAFKA][ANALITICA] Stock de '{}' = {} (umbral {}, bajo={}) {} (partición {}, offset {})",
        evento.nombreProducto(), evento.stockActual(), evento.umbral(), evento.stockBajo(),
        nuevo ? "registrado" : "duplicado, se descarta", registro.partition(), registro.offset());
  }

  @Override
  public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
    super.onPartitionsAssigned(assignments, callback);
    if (reconstruirAlIniciar) {
      assignments.keySet().forEach(tp -> callback.seekToBeginning(tp.topic(), tp.partition()));
      log.info("[KAFKA][ANALITICA] Reconstruyendo el estado de inventario desde el inicio de {}", assignments.keySet());
    }
  }
}
