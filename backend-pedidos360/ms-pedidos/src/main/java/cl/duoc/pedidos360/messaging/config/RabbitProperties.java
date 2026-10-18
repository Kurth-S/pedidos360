package cl.duoc.pedidos360.messaging.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Nombres de exchanges, colas y routing keys leídos desde application.yml
 * (prefijo {@code pedidos360.rabbit}). Es la única fuente de estos nombres en el servicio.
 */
@ConfigurationProperties(prefix = "pedidos360.rabbit")
public record RabbitProperties(
    String exchange,
    String deadLetterExchange,
    RoutingKeys routingKeys,
    Queues queues) {

  public record RoutingKeys(String pedidoCreado, String stockBajo) {}

  /** Una cola de trabajo y su cola de mensajes muertos (DLQ). */
  public record QueueNames(String name, String dlq) {}

  public record Queues(QueueNames notificaciones, QueueNames tickets, QueueNames alertasStock) {}
}
