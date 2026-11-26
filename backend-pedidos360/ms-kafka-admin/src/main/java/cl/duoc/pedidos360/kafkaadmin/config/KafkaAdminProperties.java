package cl.duoc.pedidos360.kafkaadmin.config;

import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Parámetros del administrador de Kafka (prefijo {@code pedidos360.kafka-admin} en application.yml).
 *
 * <ul>
 *   <li>bootstrapServers: brokers del clúster (los levanta docker-compose.kafka.yml).</li>
 *   <li>requestTimeoutMs: tiempo máximo de espera de cada operación del AdminClient.</li>
 *   <li>bootstrapTopics: si es true, al iniciar crea/alinea los tópicos de {@code topics} (idempotente).</li>
 *   <li>topics: definición declarativa de los tópicos del sistema (particiones, réplicas y configuración).</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "pedidos360.kafka-admin")
public record KafkaAdminProperties(
    String bootstrapServers,
    @DefaultValue("10000") int requestTimeoutMs,
    @DefaultValue("true") boolean bootstrapTopics,
    List<TopicDefinition> topics) {

  public KafkaAdminProperties {
    if (bootstrapServers == null || bootstrapServers.isBlank()) {
      bootstrapServers = "localhost:9092";
    }
    if (topics == null) {
      topics = List.of();
    }
  }

  /** Un tópico: nombre, particiones, factor de replicación y configuración (retention.ms, cleanup.policy, ...). */
  public record TopicDefinition(String name, int partitions, int replicationFactor, Map<String, String> configs) {
    public TopicDefinition {
      if (configs == null) {
        configs = Map.of();
      }
    }
  }
}
