package cl.duoc.pedidos360.kafkaadmin.dto;

/** Dónde quedó el mensaje de prueba. */
public record PublishResult(String topic, int partition, long offset) {}
