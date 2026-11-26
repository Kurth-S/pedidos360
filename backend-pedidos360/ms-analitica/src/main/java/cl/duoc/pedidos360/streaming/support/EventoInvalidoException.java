package cl.duoc.pedidos360.streaming.support;

/**
 * El evento llegó bien pero su contenido no sirve (campos obligatorios vacíos, tipo desconocido...).
 * Reintentar no lo arregla: el manejador de errores lo envía directo a la Dead Letter Topic.
 */
public class EventoInvalidoException extends RuntimeException {
  public EventoInvalidoException(String message) {
    super(message);
  }
}
