package cl.duoc.pedidos360.kafkaadmin.service;

import static cl.duoc.pedidos360.kafkaadmin.support.KafkaFutures.esperar;

import cl.duoc.pedidos360.common.BusinessException;
import cl.duoc.pedidos360.common.ResourceNotFoundException;
import cl.duoc.pedidos360.kafkaadmin.config.KafkaAdminProperties;
import cl.duoc.pedidos360.kafkaadmin.dto.GroupDetail;
import cl.duoc.pedidos360.kafkaadmin.dto.GroupSummary;
import cl.duoc.pedidos360.kafkaadmin.dto.ResetOffsetsRequest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/** Inspección y mantenimiento de consumer groups: listar, ver miembros y lag, reiniciar offsets y eliminar. */
@Service
public class ConsumerGroupAdminService {

  private static final Logger log = LoggerFactory.getLogger(ConsumerGroupAdminService.class);

  private final Admin admin;
  private final int timeout;

  public ConsumerGroupAdminService(Admin admin, KafkaAdminProperties props) {
    this.admin = admin;
    this.timeout = props.requestTimeoutMs();
  }

  public List<GroupSummary> listar() {
    return esperar(admin.listConsumerGroups().all(), timeout, "grupos").stream()
        .sorted(Comparator.comparing(ConsumerGroupListing::groupId))
        .map(g -> new GroupSummary(g.groupId(), g.state().map(Object::toString).orElse("UNKNOWN")))
        .toList();
  }

  public GroupDetail detalle(String groupId) {
    ConsumerGroupDescription d = describir(groupId);

    List<GroupDetail.Member> miembros = new ArrayList<>();
    for (MemberDescription m : d.members()) {
      List<String> asignadas = m.assignment().topicPartitions().stream()
          .map(tp -> tp.topic() + "-" + tp.partition())
          .sorted()
          .toList();
      miembros.add(new GroupDetail.Member(m.consumerId(), m.clientId(), m.host(), asignadas));
    }

    Map<TopicPartition, OffsetAndMetadata> confirmados =
        esperar(admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(), timeout, groupId);
    Map<TopicPartition, Long> finales = offsetsFinales(confirmados.keySet(), OffsetSpec.latest(), groupId);

    List<GroupDetail.OffsetLag> offsets = new ArrayList<>();
    long lagTotal = 0;
    for (Map.Entry<TopicPartition, OffsetAndMetadata> e : confirmados.entrySet()) {
      if (e.getValue() == null) {
        continue;
      }
      long actual = e.getValue().offset();
      long fin = finales.getOrDefault(e.getKey(), actual);
      long lag = Math.max(0, fin - actual);
      lagTotal += lag;
      offsets.add(new GroupDetail.OffsetLag(e.getKey().topic(), e.getKey().partition(), actual, fin, lag));
    }
    offsets.sort(Comparator.comparing(GroupDetail.OffsetLag::topic).thenComparingInt(GroupDetail.OffsetLag::partition));

    String coordinador = d.coordinator() == null ? "" : d.coordinator().host() + ":" + d.coordinator().port();
    return new GroupDetail(d.groupId(), String.valueOf(d.state()), d.partitionAssignor(), coordinador,
        miembros, offsets, lagTotal);
  }

  public void eliminar(String groupId) {
    esperar(admin.deleteConsumerGroups(List.of(groupId)).all(), timeout, groupId);
    log.info("[KAFKA-ADMIN] Consumer group '{}' eliminado", groupId);
  }

  /** Mueve los offsets del grupo al inicio o al final del tópico. El grupo no debe tener consumidores activos. */
  public void reiniciarOffsets(String groupId, ResetOffsetsRequest req) {
    ConsumerGroupDescription d = describir(groupId);
    if (!d.members().isEmpty()) {
      throw new BusinessException(HttpStatus.CONFLICT, "El grupo '" + groupId + "' tiene "
          + d.members().size() + " consumidor(es) activo(s): detén los servicios antes de reiniciar offsets");
    }

    TopicDescription topico = esperar(admin.describeTopics(List.of(req.topic())).allTopicNames(), timeout, req.topic())
        .get(req.topic());
    List<TopicPartition> particiones = topico.partitions().stream()
        .map(p -> new TopicPartition(req.topic(), p.partition()))
        .toList();

    OffsetSpec posicion = req.to() == ResetOffsetsRequest.Position.EARLIEST ? OffsetSpec.earliest() : OffsetSpec.latest();
    Map<TopicPartition, Long> destino = offsetsFinales(particiones, posicion, req.topic());

    Map<TopicPartition, OffsetAndMetadata> nuevos = new HashMap<>();
    destino.forEach((tp, offset) -> nuevos.put(tp, new OffsetAndMetadata(offset)));
    esperar(admin.alterConsumerGroupOffsets(groupId, nuevos).all(), timeout, groupId);
    log.info("[KAFKA-ADMIN] Offsets del grupo '{}' en '{}' movidos a {}", groupId, req.topic(), req.to());
  }

  private ConsumerGroupDescription describir(String groupId) {
    ConsumerGroupDescription d = esperar(admin.describeConsumerGroups(List.of(groupId)).all(), timeout, groupId).get(groupId);
    // Kafka responde un grupo "DEAD" (sin error) cuando el id no existe
    if (d == null || "DEAD".equals(String.valueOf(d.state()))) {
      throw new ResourceNotFoundException("El consumer group '" + groupId + "' no existe");
    }
    return d;
  }

  private Map<TopicPartition, Long> offsetsFinales(Iterable<TopicPartition> particiones, OffsetSpec spec, String recurso) {
    Map<TopicPartition, OffsetSpec> consulta = new HashMap<>();
    particiones.forEach(tp -> consulta.put(tp, spec));
    Map<TopicPartition, Long> resultado = new HashMap<>();
    if (consulta.isEmpty()) {
      return resultado;
    }
    Map<TopicPartition, ListOffsetsResultInfo> info = esperar(admin.listOffsets(consulta).all(), timeout, recurso);
    info.forEach((tp, r) -> resultado.put(tp, r.offset()));
    return resultado;
  }
}
