package cl.duoc.pedidos360.streaming.support;

/** No se pudo confirmar la publicación en Kafka. Es transitoria: el consumidor reintenta con espera exponencial. */
public class KafkaPublicacionException extends RuntimeException {
  public KafkaPublicacionException(String message, Throwable cause) {
    super(message, cause);
  }
}
