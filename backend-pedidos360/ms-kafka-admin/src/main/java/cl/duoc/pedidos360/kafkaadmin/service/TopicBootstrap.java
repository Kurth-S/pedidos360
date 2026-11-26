package cl.duoc.pedidos360.kafkaadmin.service;

import cl.duoc.pedidos360.kafkaadmin.config.KafkaAdminProperties;
import cl.duoc.pedidos360.kafkaadmin.config.KafkaAdminProperties.TopicDefinition;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Infraestructura como código: al iniciar, crea los tópicos declarados en application.yml
 * (pedidos360.kafka-admin.topics) o, si ya existen, alinea su configuración. Es idempotente.
 *
 * <p>Si el clúster tiene menos brokers que el factor de replicación pedido (por ejemplo, un solo broker
 * en desarrollo), usa los brokers disponibles y lo deja registrado en el log.
 */
@Component
public class TopicBootstrap implements ApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(TopicBootstrap.class);
  private static final int INTENTOS = 3;
  private static final long ESPERA_MS = 5000;

  private final KafkaAdminProperties props;
  private final TopicAdminService topicos;

  public TopicBootstrap(KafkaAdminProperties props, TopicAdminService topicos) {
    this.props = props;
    this.topicos = topicos;
  }

  @Override
  public void run(ApplicationArguments args) throws InterruptedException {
    if (!props.bootstrapTopics() || props.topics().isEmpty()) {
      log.info("[KAFKA-ADMIN] Creación automática de tópicos desactivada");
      return;
    }
    for (int intento = 1; intento <= INTENTOS; intento++) {
      try {
        prepararTopicos();
        return;
      } catch (RuntimeException ex) {
        log.error("[KAFKA-ADMIN] No se pudieron preparar los tópicos (intento {}/{}): {}", intento, INTENTOS, ex.getMessage());
        if (intento < INTENTOS) {
          Thread.sleep(ESPERA_MS);
        }
      }
    }
    log.error("[KAFKA-ADMIN] Los tópicos NO quedaron listos: levanta el clúster y reinicia el servicio, o créalos con POST /api/kafka/topics");
  }

  private void prepararTopicos() {
    int brokers = topicos.cantidadBrokers();
    Set<String> existentes = topicos.nombres();
    for (TopicDefinition def : props.topics()) {
      int replicas = Math.min(def.replicationFactor(), brokers);
      if (replicas < def.replicationFactor()) {
        log.warn("[KAFKA-ADMIN] '{}' pide factor de replicación {} pero solo hay {} broker(s): se usa {}",
            def.name(), def.replicationFactor(), brokers, replicas);
      }
      Map<String, String> configs = new HashMap<>(def.configs());
      configs.computeIfPresent("min.insync.replicas", (k, v) -> String.valueOf(Math.min(Integer.parseInt(v.trim()), replicas)));

      if (existentes.contains(def.name())) {
        if (!configs.isEmpty()) {
          topicos.modificarConfiguracion(def.name(), configs);
        }
        log.info("[KAFKA-ADMIN] Tópico '{}' ya existía: configuración alineada con application.yml", def.name());
      } else {
        topicos.crear(def.name(), def.partitions(), replicas, configs);
      }
    }
  }
}
