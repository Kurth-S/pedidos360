package cl.duoc.pedidos360.messaging.support;

/** Error NO recuperable: reintentar no sirve (datos incompletos o incorrectos). Va directo a la DLQ. */
public class MensajeInvalidoException extends RuntimeException {
  public MensajeInvalidoException(String message) {
    super(message);
  }
}
