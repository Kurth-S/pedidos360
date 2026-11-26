package cl.duoc.pedidos360.kafkaadmin.dto;

import java.util.List;
import java.util.Map;

/** Detalle de un tópico: particiones (líder, réplicas, ISR) y configuraciones propias del tópico. */
public record TopicDetail(
    String name,
    boolean internal,
    int replicationFactor,
    List<PartitionDetail> partitions,
    Map<String, String> configs) {

  public record PartitionDetail(int partition, int leader, List<Integer> replicas, List<Integer> isr) {}
}
