package cl.duoc.pedidos360.streaming.consumer;

import cl.duoc.pedidos360.analitica.AnaliticaStore;
import cl.duoc.pedidos360.streaming.event.PedidoEventoDto;
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
 * Consumer group {@code analitica.ventas}: lee el tópico de pedidos y acumula ventas, ingresos y productos más vendidos.
 *
 * <p>Los indicadores viven en memoria; para no perderlos al reiniciar, al recibir sus particiones el consumidor
 * vuelve al inicio del tópico y reconstruye todo desde Kafka (replay). El almacén es idempotente por eventId,
 * así que reprocesar eventos ya contados no duplica nada.
 */
@Component
public class PedidosAnaliticaConsumer extends AbstractConsumerSeekAware {

  private static final Logger log = LoggerFactory.getLogger(PedidosAnaliticaConsumer.class);

  private final ValidadorEventos validador;
  private final AnaliticaStore store;
  private final boolean reconstruirAlIniciar;

  public PedidosAnaliticaConsumer(ValidadorEventos validador, AnaliticaStore store,
                                  @Value("${pedidos360.analitica.reconstruir-al-iniciar:true}") boolean reconstruirAlIniciar) {
    this.validador = validador;
    this.store = store;
    this.reconstruirAlIniciar = reconstruirAlIniciar;
  }

  @KafkaListener(
      topics = "${pedidos360.kafka.topics.pedidos-eventos}",
      groupId = "${pedidos360.kafka.groups.analitica-ventas}",
      containerFactory = "pedidosKafkaListenerFactory")
  public void alRecibirPedido(ConsumerRecord<String, PedidoEventoDto> registro) {
    PedidoEventoDto evento = validador.validar(registro.value(), "Evento de pedido");
    if (!PedidoEventoDto.PEDIDO_CREADO.equals(evento.tipo())) {
      throw new EventoInvalidoException("Tipo de evento de pedido desconocido: " + evento.tipo());
    }
    boolean nuevo = store.registrarPedido(evento);
    log.info("[KAFKA][ANALITICA] Pedido {} {} (partición {}, offset {}, total {})", evento.pedidoId(),
        nuevo ? "contabilizado" : "ya contabilizado, se descarta", registro.partition(), registro.offset(), evento.total());
  }

  @Override
  public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
    super.onPartitionsAssigned(assignments, callback);
    if (reconstruirAlIniciar) {
      assignments.keySet().forEach(tp -> callback.seekToBeginning(tp.topic(), tp.partition()));
      log.info("[KAFKA][ANALITICA] Reconstruyendo indicadores de ventas desde el inicio de {}", assignments.keySet());
    }
  }
}
