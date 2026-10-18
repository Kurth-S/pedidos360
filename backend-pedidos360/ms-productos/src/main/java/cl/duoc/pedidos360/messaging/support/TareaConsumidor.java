package cl.duoc.pedidos360.messaging.support;

/** Trabajo que realiza un consumidor sobre un mensaje; puede lanzar cualquier excepción. */
@FunctionalInterface
public interface TareaConsumidor {
  void ejecutar() throws Exception;
}
