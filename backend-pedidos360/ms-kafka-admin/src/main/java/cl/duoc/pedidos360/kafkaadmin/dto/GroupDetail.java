package cl.duoc.pedidos360.kafkaadmin.dto;

import java.util.List;

/** Estado de un consumer group: miembros, partición asignada y retraso (lag) por partición. */
public record GroupDetail(
    String groupId,
    String state,
    String assignor,
    String coordinator,
    List<Member> members,
    List<OffsetLag> offsets,
    long totalLag) {

  public record Member(String consumerId, String clientId, String host, List<String> assignment) {}

  public record OffsetLag(String topic, int partition, long currentOffset, long endOffset, long lag) {}
}
