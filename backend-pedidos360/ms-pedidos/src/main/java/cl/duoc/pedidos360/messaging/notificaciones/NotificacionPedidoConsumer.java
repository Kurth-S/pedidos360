package cl.duoc.pedidos360.messaging.notificaciones;

import cl.duoc.pedidos360.messaging.event.PedidoCreadoEvent;
import cl.duoc.pedidos360.messaging.support.ConfirmacionMensajes;
import cl.duoc.pedidos360.messaging.support.MensajeInvalidoException;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/** Dominio "notificaciones": avisa al cliente que su pedido fue recibido (simulado en el log). */
@Component
public class NotificacionPedidoConsumer {

  private static final Logger log = LoggerFactory.getLogger(NotificacionPedidoConsumer.class);

  private final ConfirmacionMensajes confirmacion;

  public NotificacionPedidoConsumer(ConfirmacionMensajes confirmacion) {
    this.confirmacion = confirmacion;
  }

  @RabbitListener(queues = "${pedidos360.rabbit.queues.notificaciones.name}")
  public void alCrearsePedido(PedidoCreadoEvent evento,
                              Channel channel,
                              @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                              @Header(name = AmqpHeaders.REDELIVERED, defaultValue = "false") boolean redelivered) {
    confirmacion.procesar("notificaciones", evento, channel, tag, redelivered, () -> {
      if (evento.clienteEmail() == null || evento.clienteEmail().isBlank()) {
        throw new MensajeInvalidoException("El pedido " + evento.pedidoId() + " no trae el correo del cliente");
      }
      log.info("[NOTIFICACION] Correo enviado a {}: tu pedido #{} por {} fue recibido",
          evento.clienteEmail(), evento.pedidoId(), evento.total());
    });
  }
}
