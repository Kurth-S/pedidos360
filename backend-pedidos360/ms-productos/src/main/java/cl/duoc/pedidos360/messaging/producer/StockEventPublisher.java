package cl.duoc.pedidos360.messaging.producer;

import cl.duoc.pedidos360.messaging.config.RabbitProperties;
import cl.duoc.pedidos360.messaging.event.StockBajoEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

/**
 * Productor: publica la alerta de stock bajo. Se llama desde un consumidor, por lo que un fallo
 * de publicación se propaga y el consumidor lo maneja (reintento y DLQ).
 */
@Component
public class StockEventPublisher {

  private static final Logger log = LoggerFactory.getLogger(StockEventPublisher.class);

  private final RabbitTemplate rabbitTemplate;
  private final RabbitProperties props;

  public StockEventPublisher(RabbitTemplate rabbitTemplate, RabbitProperties props) {
    this.rabbitTemplate = rabbitTemplate;
    this.props = props;
  }

  public void publicarStockBajo(StockBajoEvent evento) {
    rabbitTemplate.convertAndSend(props.exchange(), props.routingKeys().stockBajo(), evento);
    log.info("Evento '{}' publicado para el producto {}", props.routingKeys().stockBajo(), evento.productoId());
  }
}
