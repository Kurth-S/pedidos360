package cl.duoc.pedidos360.kafkaadmin.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Clientes Kafka del servicio: {@link Admin} (administración) y un productor de texto para mensajes de prueba. */
@Configuration
public class KafkaAdminConfig {

  @Bean(destroyMethod = "close")
  Admin kafkaAdminClient(KafkaAdminProperties props) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
    cfg.put(AdminClientConfig.CLIENT_ID_CONFIG, "ms-kafka-admin");
    cfg.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, props.requestTimeoutMs());
    cfg.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, props.requestTimeoutMs());
    cfg.put(AdminClientConfig.RETRIES_CONFIG, 3);
    return Admin.create(cfg);
  }

  /** Productor de texto: permite publicar un JSON (válido o roto) en cualquier tópico para probar los consumidores y la DLT. */
  @Bean(destroyMethod = "close")
  KafkaProducer<String, String> pruebaProducer(KafkaAdminProperties props) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
    cfg.put(ProducerConfig.CLIENT_ID_CONFIG, "ms-kafka-admin-prueba");
    cfg.put(ProducerConfig.ACKS_CONFIG, "all");
    cfg.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, props.requestTimeoutMs());
    cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, props.requestTimeoutMs());
    cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, props.requestTimeoutMs() * 2);
    return new KafkaProducer<>(cfg, new StringSerializer(), new StringSerializer());
  }
}
