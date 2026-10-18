package cl.duoc.pedidos360.messaging.support;

import com.rabbitmq.client.Channel;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Política de confirmación (ACK) común a todos los consumidores:
 *
 * <table>
 *   <tr><td>Tarea OK</td><td>ACK: el mensaje se elimina de la cola.</td></tr>
 *   <tr><td>MensajeInvalidoException</td><td>NACK sin reencolar: va a la DLQ y se registra en el log.</td></tr>
 *   <tr><td>Otro error, primera vez</td><td>NACK con reencolar: un segundo intento (falla transitoria).</td></tr>
 *   <tr><td>Otro error, ya reentregado</td><td>NACK sin reencolar: va a la DLQ y se registra en el log.</td></tr>
 * </table>
 */
@Component
public class ConfirmacionMensajes {

  private static final Logger log = LoggerFactory.getLogger(ConfirmacionMensajes.class);

  public void procesar(String consumidor, Object mensaje, Channel channel, long deliveryTag,
                       boolean redelivered, TareaConsumidor tarea) {
    try {
      tarea.ejecutar();
    } catch (MensajeInvalidoException ex) {
      log.error("[{}] Mensaje inválido, no se reintenta. Se envía a la DLQ. Motivo: {} | Mensaje: {}",
          consumidor, ex.getMessage(), mensaje);
      nack(channel, deliveryTag, false);
      return;
    } catch (Exception ex) {
      if (!redelivered) {
        log.warn("[{}] Falla transitoria ({}). Se reencola para un segundo intento. Mensaje: {}",
            consumidor, ex.getMessage(), mensaje);
        nack(channel, deliveryTag, true);
      } else {
        log.error("[{}] Falla tras el reintento ({}). Se envía a la DLQ. Mensaje: {}",
            consumidor, ex.getMessage(), mensaje);
        nack(channel, deliveryTag, false);
      }
      return;
    }
    ack(channel, deliveryTag, consumidor);
  }

  private void ack(Channel channel, long deliveryTag, String consumidor) {
    try {
      channel.basicAck(deliveryTag, false);
    } catch (IOException e) {
      log.error("[{}] No se pudo enviar el ACK: {}", consumidor, e.getMessage());
    }
  }

  private void nack(Channel channel, long deliveryTag, boolean requeue) {
    try {
      channel.basicNack(deliveryTag, false, requeue);
    } catch (IOException e) {
      log.error("No se pudo enviar el NACK: {}", e.getMessage());
    }
  }
}
