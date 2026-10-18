package cl.duoc.pedidos360.messaging.tickets;

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

/** Dominio "tickets": genera el ticket del pedido de forma asíncrona. */
@Component
public class TicketPedidoConsumer {

  private static final Logger log = LoggerFactory.getLogger(TicketPedidoConsumer.class);

  private final ConfirmacionMensajes confirmacion;
  private final GeneradorTicket generador;

  public TicketPedidoConsumer(ConfirmacionMensajes confirmacion, GeneradorTicket generador) {
    this.confirmacion = confirmacion;
    this.generador = generador;
  }

  @RabbitListener(queues = "${pedidos360.rabbit.queues.tickets.name}")
  public void alCrearsePedido(PedidoCreadoEvent evento,
                              Channel channel,
                              @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                              @Header(name = AmqpHeaders.REDELIVERED, defaultValue = "false") boolean redelivered) {
    confirmacion.procesar("tickets", evento, channel, tag, redelivered, () -> {
      if (evento.pedidoId() == null || evento.items() == null || evento.items().isEmpty()) {
        throw new MensajeInvalidoException("El pedido no trae id o ítems para generar el ticket");
      }
      log.info("[TICKET] {}", generador.generar(evento));
    });
  }
}
