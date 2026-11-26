package cl.duoc.pedidos360.kafkaadmin.dto;

public record TopicSummary(String name, int partitions, int replicationFactor, boolean internal) {}
