package cl.duoc.pedidos360.streaming.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Parámetros del cliente Kafka (prefijo {@code pedidos360.kafka} en application.yml).
 * Es la única fuente de nombres de tópicos, consumer groups y propiedades avanzadas del servicio.
 */
@ConfigurationProperties(prefix = "pedidos360.kafka")
public record KafkaSettings(
    @DefaultValue("localhost:9092") String bootstrapServers,
    @DefaultValue Topics topics,
    @DefaultValue Groups groups,
    @DefaultValue Producer producer,
    @DefaultValue Consumer consumer,
    @DefaultValue Retry retry) {

  /** Tópicos de eventos y sufijo de los Dead Letter Topics. */
  public record Topics(
      @DefaultValue("pedidos360.pedidos.eventos") String pedidosEventos,
      @DefaultValue("pedidos360.inventario.eventos") String inventarioEventos,
      @DefaultValue(".DLT") String dltSuffix) {}

  /** Consumer groups: cada grupo recibe TODOS los mensajes del tópico; dentro de un grupo se reparten las particiones. */
  public record Groups(
      @DefaultValue("productos.inventario-demanda") String inventarioDemanda,
      @DefaultValue("analitica.ventas") String analiticaVentas,
      @DefaultValue("analitica.inventario") String analiticaInventario) {}

  /** Garantías de entrega del productor. */
  public record Producer(
      @DefaultValue("all") String acks,
      @DefaultValue("true") boolean idempotence,
      @DefaultValue("5") int retries,
      @DefaultValue("3000") int maxBlockMs,
      @DefaultValue("5000") int requestTimeoutMs,
      @DefaultValue("15000") int deliveryTimeoutMs,
      @DefaultValue("5") int lingerMs,
      @DefaultValue("lz4") String compression) {}

  /** Comportamiento de los consumidores. */
  public record Consumer(
      @DefaultValue("earliest") String autoOffsetReset,
      @DefaultValue("50") int maxPollRecords,
      @DefaultValue("300000") int maxPollIntervalMs,
      @DefaultValue("30000") int sessionTimeoutMs,
      @DefaultValue("10000") int heartbeatIntervalMs,
      @DefaultValue("3") int concurrency) {}

  /** Reintentos con espera exponencial antes de enviar un mensaje a la DLT. */
  public record Retry(
      @DefaultValue("3") int maxAttempts,
      @DefaultValue("500") long initialIntervalMs,
      @DefaultValue("2.0") double multiplier,
      @DefaultValue("5000") long maxIntervalMs) {}
}
