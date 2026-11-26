package cl.duoc.pedidos360.kafkaadmin.service;

import static cl.duoc.pedidos360.kafkaadmin.support.KafkaFutures.esperar;

import cl.duoc.pedidos360.common.BusinessException;
import cl.duoc.pedidos360.kafkaadmin.config.KafkaAdminProperties;
import cl.duoc.pedidos360.kafkaadmin.dto.ClusterInfo;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicDetail;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicSummary;
import cl.duoc.pedidos360.kafkaadmin.support.TopicConfigRules;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/** Operaciones de administración sobre el clúster y sus tópicos (crear, listar, modificar, eliminar). */
@Service
public class TopicAdminService {

  private static final Logger log = LoggerFactory.getLogger(TopicAdminService.class);

  private final Admin admin;
  private final int timeout;

  public TopicAdminService(Admin admin, KafkaAdminProperties props) {
    this.admin = admin;
    this.timeout = props.requestTimeoutMs();
  }

  public ClusterInfo cluster() {
    DescribeClusterResult r = admin.describeCluster();
    String id = esperar(r.clusterId(), timeout, "cluster");
    Collection<Node> nodos = esperar(r.nodes(), timeout, "cluster");
    Node controlador = esperar(r.controller(), timeout, "cluster");
    List<ClusterInfo.Broker> brokers = nodos.stream()
        .map(n -> new ClusterInfo.Broker(n.id(), n.host(), n.port()))
        .sorted(Comparator.comparingInt(ClusterInfo.Broker::id))
        .toList();
    return new ClusterInfo(id, controlador == null ? -1 : controlador.id(), brokers);
  }

  public int cantidadBrokers() {
    return esperar(admin.describeCluster().nodes(), timeout, "cluster").size();
  }

  public List<TopicSummary> listar() {
    Set<String> nombres = esperar(admin.listTopics(new ListTopicsOptions().listInternal(false)).names(), timeout, "tópicos");
    if (nombres.isEmpty()) {
      return List.of();
    }
    Map<String, TopicDescription> descripciones =
        esperar(admin.describeTopics(nombres).allTopicNames(), timeout, "tópicos");
    return descripciones.values().stream()
        .sorted(Comparator.comparing(TopicDescription::name))
        .map(t -> new TopicSummary(t.name(), t.partitions().size(), factorReplicacion(t), t.isInternal()))
        .toList();
  }

  public TopicDetail detalle(String nombre) {
    TopicDescription d = esperar(admin.describeTopics(List.of(nombre)).allTopicNames(), timeout, nombre).get(nombre);
    List<TopicDetail.PartitionDetail> particiones = d.partitions().stream()
        .map(p -> new TopicDetail.PartitionDetail(
            p.partition(),
            p.leader() == null ? -1 : p.leader().id(),
            p.replicas().stream().map(Node::id).toList(),
            p.isr().stream().map(Node::id).toList()))
        .toList();
    return new TopicDetail(d.name(), d.isInternal(), factorReplicacion(d), particiones, configuracionPropia(nombre));
  }

  public void crear(TopicRequest req) {
    crear(req.name(), req.partitions(), req.replicationFactor(), req.configs());
  }

  /** Crea un tópico (también lo usa el arranque para los tópicos declarados en application.yml). */
  public void crear(String nombre, int particiones, int replicas, Map<String, String> configs) {
    TopicConfigRules.validar(configs);
    NewTopic topico = new NewTopic(nombre, particiones, (short) replicas);
    if (configs != null && !configs.isEmpty()) {
      topico.configs(configs);
    }
    esperar(admin.createTopics(List.of(topico)).all(), timeout, nombre);
    log.info("[KAFKA-ADMIN] Tópico '{}' creado ({} particiones, factor de replicación {}, configs {})",
        nombre, particiones, replicas, configs);
  }

  /** Fija configuraciones del tópico (retention.ms, cleanup.policy, ...) sin tocar las demás. */
  public void modificarConfiguracion(String nombre, Map<String, String> configs) {
    TopicConfigRules.validar(configs);
    ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
    Collection<AlterConfigOp> operaciones = configs.entrySet().stream()
        .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue().trim()), AlterConfigOp.OpType.SET))
        .toList();
    Map<ConfigResource, Collection<AlterConfigOp>> cambios = Map.of(recurso, operaciones);
    esperar(admin.incrementalAlterConfigs(cambios).all(), timeout, nombre);
    log.info("[KAFKA-ADMIN] Configuración del tópico '{}' actualizada: {}", nombre, configs);
  }

  /** Quita un override: el tópico vuelve al valor por defecto del broker para esa clave. */
  public void restablecerConfiguracion(String nombre, String clave) {
    TopicConfigRules.validarClave(clave);
    ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
    Collection<AlterConfigOp> operaciones =
        List.of(new AlterConfigOp(new ConfigEntry(clave, null), AlterConfigOp.OpType.DELETE));
    Map<ConfigResource, Collection<AlterConfigOp>> cambios = Map.of(recurso, operaciones);
    esperar(admin.incrementalAlterConfigs(cambios).all(), timeout, nombre);
    log.info("[KAFKA-ADMIN] Configuración '{}' del tópico '{}' restablecida al valor por defecto", clave, nombre);
  }

  /** Aumenta el número total de particiones (Kafka no permite reducirlas). */
  public void aumentarParticiones(String nombre, int total) {
    esperar(admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(total))).all(), timeout, nombre);
    log.info("[KAFKA-ADMIN] Tópico '{}' ahora tiene {} particiones", nombre, total);
  }

  public void eliminar(String nombre) {
    if (nombre.startsWith("__")) {
      throw new BusinessException(HttpStatus.BAD_REQUEST, "No se pueden eliminar tópicos internos de Kafka");
    }
    esperar(admin.deleteTopics(List.of(nombre)).all(), timeout, nombre);
    log.info("[KAFKA-ADMIN] Tópico '{}' eliminado", nombre);
  }

  /** Nombres de todos los tópicos existentes (para el arranque idempotente). */
  public Set<String> nombres() {
    return esperar(admin.listTopics().names(), timeout, "tópicos");
  }

  private Map<String, String> configuracionPropia(String nombre) {
    ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
    Config config = esperar(admin.describeConfigs(List.of(recurso)).all(), timeout, nombre).get(recurso);
    Map<String, String> propias = new TreeMap<>();
    for (ConfigEntry e : config.entries()) {
      if (e.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
        propias.put(e.name(), e.value());
      }
    }
    return propias;
  }

  private static int factorReplicacion(TopicDescription t) {
    return t.partitions().isEmpty() ? 0 : t.partitions().get(0).replicas().size();
  }
}
