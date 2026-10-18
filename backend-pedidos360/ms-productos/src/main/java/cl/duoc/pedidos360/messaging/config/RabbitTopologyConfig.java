package cl.duoc.pedidos360.messaging.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topología RabbitMQ de ms-productos. Solo declara infraestructura, sin lógica de negocio.
 *
 * <pre>
 *  pedidos360.eventos (topic)
 *    └─ pedido.creado ► inventario.pedido-creado.q   (caso 4: revisar stock de lo pedido)
 *  pedidos360.eventos.dlx (direct): inventario.pedido-creado.dlq recibe los rechazos
 * </pre>
 *
 * <p>El exchange se declara igual que en ms-pedidos (mismos parámetros), así ambos servicios
 * pueden iniciarse en cualquier orden.
 */
@Configuration
public class RabbitTopologyConfig {

  private final RabbitProperties props;

  public RabbitTopologyConfig(RabbitProperties props) {
    this.props = props;
  }

  @Bean
  TopicExchange eventosExchange() {
    return new TopicExchange(props.exchange(), true, false);
  }

  @Bean
  DirectExchange deadLetterExchange() {
    return new DirectExchange(props.deadLetterExchange(), true, false);
  }

  @Bean
  Queue inventarioQueue() {
    return QueueBuilder.durable(props.queues().inventario().name())
        .deadLetterExchange(props.deadLetterExchange())
        .deadLetterRoutingKey(props.queues().inventario().dlq())
        .build();
  }

  @Bean
  Queue inventarioDlq() {
    return QueueBuilder.durable(props.queues().inventario().dlq()).build();
  }

  @Bean
  Binding inventarioBinding() {
    return BindingBuilder.bind(inventarioQueue()).to(eventosExchange()).with(props.routingKeys().pedidoCreado());
  }

  @Bean
  Binding inventarioDlqBinding() {
    return BindingBuilder.bind(inventarioDlq()).to(deadLetterExchange()).with(props.queues().inventario().dlq());
  }
}
