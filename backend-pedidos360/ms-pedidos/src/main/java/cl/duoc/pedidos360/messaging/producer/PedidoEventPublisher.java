package cl.duoc.pedidos360.messaging.producer;

import cl.duoc.pedidos360.messaging.config.RabbitProperties;
import cl.duoc.pedidos360.messaging.event.PedidoCreadoEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Productor: publica eventos de pedidos en el exchange.
 *
 * <p>Dos garantías para no afectar la lógica existente:
 * <ul>
 *   <li>El evento se envía solo después del COMMIT, así nunca se anuncia un pedido que no quedó guardado.</li>
 *   <li>Si RabbitMQ no está disponible, se registra el error y el pedido igualmente se crea.</li>
 * </ul>
 */
@Component
public class PedidoEventPublisher {

  private static final Logger log = LoggerFactory.getLogger(PedidoEventPublisher.class);

  private final RabbitTemplate rabbitTemplate;
  private final RabbitProperties props;

  public PedidoEventPublisher(RabbitTemplate rabbitTemplate, RabbitProperties props) {
    this.rabbitTemplate = rabbitTemplate;
    this.props = props;
  }

  public void publicarPedidoCreado(PedidoCreadoEvent evento) {
    String routingKey = props.routingKeys().pedidoCreado();
    Runnable envio = () -> {
      try {
        rabbitTemplate.convertAndSend(props.exchange(), routingKey, evento);
        log.info("Evento '{}' publicado para el pedido {}", routingKey, evento.pedidoId());
      } catch (AmqpException ex) {
        log.error("No se pudo publicar el evento '{}' del pedido {} (el pedido ya quedó guardado): {}",
            routingKey, evento.pedidoId(), ex.getMessage());
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
