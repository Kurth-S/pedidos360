package cl.duoc.pedidos360.streaming.producer;

import cl.duoc.pedidos360.streaming.config.KafkaSettings;
import cl.duoc.pedidos360.streaming.event.PedidoEventoDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Productor Kafka: publica cada pedido creado en el tópico {@code pedidos360.pedidos.eventos}.
 *
 * <p>Convive con RabbitMQ sin afectarlo: RabbitMQ sigue repartiendo las TAREAS (notificar, ticket, inventario)
 * y Kafka guarda el REGISTRO de hechos para analítica. Mismas garantías que el publicador de RabbitMQ:
 * <ul>
 *   <li>Se envía solo después del COMMIT del pedido.</li>
 *   <li>Si Kafka no está disponible se registra el error y el pedido igualmente se crea.</li>
 * </ul>
 */
@Component
public class PedidoKafkaPublisher {

  private static final Logger log = LoggerFactory.getLogger(PedidoKafkaPublisher.class);

  private final KafkaTemplate<String, PedidoEventoDto> kafkaTemplate;
  private final KafkaSettings settings;

  public PedidoKafkaPublisher(KafkaTemplate<String, PedidoEventoDto> kafkaTemplate, KafkaSettings settings) {
    this.kafkaTemplate = kafkaTemplate;
    this.settings = settings;
  }

  public void publicarPedidoCreado(PedidoEventoDto evento) {
    String topico = settings.topics().pedidosEventos();
    String clave = String.valueOf(evento.pedidoId());
    Runnable envio = () -> {
      try {
        kafkaTemplate.send(topico, clave, evento).whenComplete((resultado, error) -> {
          if (error != null) {
            log.error("[KAFKA] No se pudo publicar {} del pedido {} en '{}' (el pedido ya quedó guardado): {}",
                evento.tipo(), evento.pedidoId(), topico, error.getMessage());
          } else {
            log.info("[KAFKA] {} del pedido {} publicado en {} (partición {}, offset {})",
                evento.tipo(), evento.pedidoId(), topico,
                resultado.getRecordMetadata().partition(), resultado.getRecordMetadata().offset());
          }
        });
      } catch (RuntimeException ex) {
        log.error("[KAFKA] No se pudo enviar {} del pedido {} a '{}' (el pedido ya quedó guardado): {}",
            evento.tipo(), evento.pedidoId(), topico, ex.getMessage());
      }
    };

    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
        @Override
        public void afterCommit() {
          envio.run();
        }
      });
    } else {
      envio.run();
    }
  }
}
