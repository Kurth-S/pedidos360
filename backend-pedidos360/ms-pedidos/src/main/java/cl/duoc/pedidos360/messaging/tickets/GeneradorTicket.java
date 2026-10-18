package cl.duoc.pedidos360.messaging.tickets;

import cl.duoc.pedidos360.messaging.event.PedidoCreadoEvent;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.springframework.stereotype.Component;

/** Arma el texto del ticket de un pedido. */
@Component
public class GeneradorTicket {

  private static final DateTimeFormatter FORMATO = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").withZone(ZoneOffset.UTC);

  public String codigo(PedidoCreadoEvent evento) {
    return "TCK-" + evento.pedidoId();
  }

  public String generar(PedidoCreadoEvent evento) {
    StringBuilder sb = new StringBuilder();
    sb.append("TICKET ").append(codigo(evento)).append(" | ")
        .append(evento.fecha() == null ? "sin fecha" : FORMATO.format(evento.fecha())).append(" UTC | ")
        .append(evento.clienteNombre() == null ? "cliente" : evento.clienteNombre()).append(" | ");
    evento.items().forEach(i -> sb.append(i.cantidad()).append("x ").append(i.nombreProducto()).append("; "));
    sb.append("TOTAL ").append(evento.total());
    return sb.toString();
  }
}
