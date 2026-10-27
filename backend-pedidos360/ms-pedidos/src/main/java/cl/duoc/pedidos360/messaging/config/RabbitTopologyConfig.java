package cl.duoc.pedidos360.messaging.config;

import cl.duoc.pedidos360.messaging.config.RabbitProperties.QueueNames;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topología RabbitMQ de ms-pedidos: un bean Exchange / Queue / Binding por cada caso de uso.
 * Solo declara infraestructura; no contiene lógica de negocio. Spring (RabbitAdmin) crea todo
 * en el broker al conectarse.
 *
 * <pre>
 *  pedidos360.eventos (topic)
 *    ├─ pedido.creado ────────► notificaciones.pedido-creado.q  (caso 1: correo al cliente)
 *    ├─ pedido.creado ────────► tickets.pedido-creado.q         (caso 2: ticket del pedido)
 *    └─ inventario.stock-bajo ► notificaciones.stock-bajo.q     (caso 3: alerta al administrador)
 *
 *  pedidos360.eventos.dlx (direct): cada cola de trabajo envía sus rechazos a su propia DLQ.
 * </pre>
 */
@Configuration
public class RabbitTopologyConfig {

  private final RabbitProperties props;

  public RabbitTopologyConfig(RabbitProperties props) {
    this.props = props;
  }

  // ---------- Exchanges ----------

  @Bean
  TopicExchange eventosExchange() {
    return new TopicExchange(props.exchange(), true, false);
  }

  @Bean
  DirectExchange deadLetterExchange() {
    return new DirectExchange(props.deadLetterExchange(), true, false);
  }

  // ---------- Caso de uso 1: notificación al cliente ----------

  @Bean
  Queue notificacionesQueue() {
    return colaConDlq(props.queues().notificaciones());
  }

  @Bean
  Queue notificacionesDlq() {
    return QueueBuilder.durable(props.queues().notificaciones().dlq()).quorum().build();
  }

  @Bean
  Binding notificacionesBinding() {
    return BindingBuilder.bind(notificacionesQueue()).to(eventosExchange()).with(props.routingKeys().pedidoCreado());
  }

  @Bean
  Binding notificacionesDlqBinding() {
    return BindingBuilder.bind(notificacionesDlq()).to(deadLetterExchange()).with(props.queues().notificaciones().dlq());
  }

  // ---------- Caso de uso 2: generación de ticket ----------

  @Bean
  Queue ticketsQueue() {
    return colaConDlq(props.queues().tickets());
  }

  @Bean
  Queue ticketsDlq() {
    return QueueBuilder.durable(props.queues().tickets().dlq()).quorum().build();
  }

  @Bean
  Binding ticketsBinding() {
    return BindingBuilder.bind(ticketsQueue()).to(eventosExchange()).with(props.routingKeys().pedidoCreado());
  }

  @Bean
  Binding ticketsDlqBinding() {
    return BindingBuilder.bind(ticketsDlq()).to(deadLetterExchange()).with(props.queues().tickets().dlq());
  }

  // ---------- Caso de uso 3: alerta de stock bajo (evento que publica ms-productos) ----------

  @Bean
  Queue alertasStockQueue() {
    return colaConDlq(props.queues().alertasStock());
  }

  @Bean
  Queue alertasStockDlq() {
    return QueueBuilder.durable(props.queues().alertasStock().dlq()).quorum().build();
  }

  @Bean
  Binding alertasStockBinding() {
    return BindingBuilder.bind(alertasStockQueue()).to(eventosExchange()).with(props.routingKeys().stockBajo());
  }

  @Bean
  Binding alertasStockDlqBinding() {
    return BindingBuilder.bind(alertasStockDlq()).to(deadLetterExchange()).with(props.queues().alertasStock().dlq());
  }

  /** Cola durable cuyos mensajes rechazados (NACK sin reencolar) se desvían a su DLQ. */
  private Queue colaConDlq(QueueNames nombres) {
    return QueueBuilder.durable(nombres.name())
        .deadLetterExchange(props.deadLetterExchange())
        .deadLetterRoutingKey(nombres.dlq())
        .quorum()
        .build();
  }
}
