package cl.duoc.pedidos360.rabbitadmin.dto;

/** Resumen de una cola existente en el broker. */
public record QueueInfo(String name, boolean durable, int messages, int consumers, String state) {}
