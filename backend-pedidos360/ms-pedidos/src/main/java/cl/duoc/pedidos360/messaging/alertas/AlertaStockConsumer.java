package cl.duoc.pedidos360.messaging.alertas;

import cl.duoc.pedidos360.messaging.event.StockBajoEvent;
import cl.duoc.pedidos360.messaging.support.ConfirmacionMensajes;
import cl.duoc.pedidos360.messaging.support.MensajeInvalidoException;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/** Dominio "alertas": avisa al administrador cuando ms-productos detecta stock bajo. */
@Component
public class AlertaStockConsumer {

  private static final Logger log = LoggerFactory.getLogger(AlertaStockConsumer.class);

  private final ConfirmacionMensajes confirmacion;

  public AlertaStockConsumer(ConfirmacionMensajes confirmacion) {
    this.confirmacion = confirmacion;
  }

  @RabbitListener(queues = "${pedidos360.rabbit.queues.alertas-stock.name}")
  public void alDetectarStockBajo(StockBajoEvent evento,
                                  Channel channel,
                                  @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                                  @Header(name = AmqpHeaders.REDELIVERED, defaultValue = "false") boolean redelivered) {
    confirmacion.procesar("alertas-stock", evento, channel, tag, redelivered, () -> {
      if (evento.productoId() == null || evento.stockActual() == null) {
        throw new MensajeInvalidoException("La alerta de stock no trae producto o stock actual");
      }
      log.warn("[ALERTA-ADMIN] Stock bajo en '{}' (id {}): quedan {} unidades (umbral {}). Detectado en el pedido #{}",
          evento.nombreProducto(), evento.productoId(), evento.stockActual(), evento.umbral(), evento.pedidoId());
    });
  }
}
