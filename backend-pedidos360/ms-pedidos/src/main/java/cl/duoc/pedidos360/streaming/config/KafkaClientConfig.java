package cl.duoc.pedidos360.streaming.config;

import cl.duoc.pedidos360.streaming.event.PedidoEventoDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * Configuración COMPLETA del cliente Kafka de ms-pedidos (productor): brokers, serializers y garantías de entrega.
 * Todos los valores salen de application.yml (pedidos360.kafka.*).
 */
@Configuration
public class KafkaClientConfig {

  /**
   * Productor de eventos de pedidos.
   * <ul>
   *   <li>Clave String (id del pedido) y valor JSON (DTO) — sin cabecera de tipo, el consumidor decide a qué DTO mapear.</li>
   *   <li>acks=all + idempotencia: el mensaje se confirma cuando lo tienen las réplicas sincronizadas y no se duplica al reintentar.</li>
   *   <li>max.block.ms bajo: si Kafka no responde, la creación del pedido no queda esperando.</li>
   * </ul>
   */
  @Bean
  public ProducerFactory<String, PedidoEventoDto> pedidosProducerFactory(KafkaSettings s, ObjectMapper objectMapper) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, s.bootstrapServers());
    cfg.put(ProducerConfig.CLIENT_ID_CONFIG, "ms-pedidos-producer");
    cfg.put(ProducerConfig.ACKS_CONFIG, s.producer().acks());
    cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, s.producer().idempotence());
    cfg.put(ProducerConfig.RETRIES_CONFIG, s.producer().retries());
    cfg.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, s.producer().maxBlockMs());
    cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, s.producer().requestTimeoutMs());
    cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, s.producer().deliveryTimeoutMs());
    cfg.put(ProducerConfig.LINGER_MS_CONFIG, s.producer().lingerMs());
    cfg.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, s.producer().compression());

    JsonSerializer<PedidoEventoDto> valor = new JsonSerializer<>(objectMapper);
    valor.setAddTypeInfo(false);
    return new DefaultKafkaProducerFactory<>(cfg, new StringSerializer(), valor);
  }

  @Bean
  public KafkaTemplate<String, PedidoEventoDto> pedidosKafkaTemplate(ProducerFactory<String, PedidoEventoDto> pedidosProducerFactory) {
    return new KafkaTemplate<>(pedidosProducerFactory);
  }
}
