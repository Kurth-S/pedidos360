package cl.duoc.pedidos360.rabbitadmin.service;

import cl.duoc.pedidos360.common.BusinessException;
import cl.duoc.pedidos360.common.ResourceNotFoundException;
import cl.duoc.pedidos360.rabbitadmin.client.RabbitManagementClient;
import cl.duoc.pedidos360.rabbitadmin.dto.BindingRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.ExchangeRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.PublishRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.QueueInfo;
import cl.duoc.pedidos360.rabbitadmin.dto.QueueRequest;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/**
 * Servicio dedicado a administrar RabbitMQ. Los controladores solo llaman a estos métodos y no
 * conocen la librería AMQP: aquí se validan las reglas, se crean/eliminan colas, exchanges y
 * bindings y se traducen los errores del broker a errores de negocio.
 */
@Service
public class RabbitAdminService {

  private static final Logger log = LoggerFactory.getLogger(RabbitAdminService.class);

  private final AmqpAdmin amqpAdmin;
  private final RabbitTemplate rabbitTemplate;
  private final RabbitManagementClient management;

  public RabbitAdminService(AmqpAdmin amqpAdmin, RabbitTemplate rabbitTemplate, RabbitManagementClient management) {
    this.amqpAdmin = amqpAdmin;
    this.rabbitTemplate = rabbitTemplate;
    this.management = management;
  }

  // ---------- Colas ----------

  public List<QueueInfo> listarColas() {
    return management.listarColas().stream()
        .map(m -> new QueueInfo(
            String.valueOf(m.get("name")),
            Boolean.TRUE.equals(m.get("durable")),
            numero(m.get("messages")),
            numero(m.get("consumers")),
            String.valueOf(m.getOrDefault("state", "desconocido"))))
        .toList();
  }

  public void crearCola(QueueRequest req) {
    validarNombreNoReservado(req.name());
    boolean hayExchangeDlq = req.deadLetterExchange() != null && !req.deadLetterExchange().isBlank();
    boolean hayRoutingKeyDlq = req.deadLetterRoutingKey() != null && !req.deadLetterRoutingKey().isBlank();
    if (hayRoutingKeyDlq && !hayExchangeDlq) {
      throw new BusinessException(HttpStatus.BAD_REQUEST, "deadLetterRoutingKey requiere deadLetterExchange");
    }

    Map<String, Object> args = new HashMap<>();
    if (hayExchangeDlq) {
      args.put("x-dead-letter-exchange", req.deadLetterExchange());
    }
    if (hayRoutingKeyDlq) {
      args.put("x-dead-letter-routing-key", req.deadLetterRoutingKey());
    }
    if (req.messageTtlMs() != null) {
      args.put("x-message-ttl", req.messageTtlMs());
    }

    try {
      amqpAdmin.declareQueue(new Queue(req.name(), req.durable(), false, false, args));
    } catch (AmqpIOException ex) {
      throw new BusinessException("La cola '" + req.name() + "' ya existe con una configuración distinta");
    }
    log.info("Cola '{}' creada (durable={}, args={})", req.name(), req.durable(), args);
  }

  public void eliminarCola(String nombre) {
    validarNombreNoReservado(nombre);
    if (amqpAdmin.getQueueProperties(nombre) == null) {
      throw new ResourceNotFoundException("La cola '" + nombre + "' no existe");
    }
    amqpAdmin.deleteQueue(nombre);
    log.info("Cola '{}' eliminada", nombre);
  }

  // ---------- Exchanges ----------

  public void crearExchange(ExchangeRequest req) {
    validarNombreNoReservado(req.name());
    ExchangeBuilder builder = switch (req.type()) {
      case DIRECT -> ExchangeBuilder.directExchange(req.name());
      case TOPIC -> ExchangeBuilder.topicExchange(req.name());
      case FANOUT -> ExchangeBuilder.fanoutExchange(req.name());
      case HEADERS -> ExchangeBuilder.headersExchange(req.name());
    };
    try {
      amqpAdmin.declareExchange(builder.durable(req.durable()).build());
    } catch (AmqpIOException ex) {
      throw new BusinessException("El exchange '" + req.name() + "' ya existe con otro tipo o configuración");
    }
    log.info("Exchange '{}' ({}) creado", req.name(), req.type());
  }

  public void eliminarExchange(String nombre) {
    validarNombreNoReservado(nombre);
    amqpAdmin.deleteExchange(nombre);
    log.info("Exchange '{}' eliminado", nombre);
  }

  // ---------- Bindings ----------

  public void crearBinding(BindingRequest req) {
    try {
      amqpAdmin.declareBinding(aBinding(req));
    } catch (AmqpIOException ex) {
      throw new ResourceNotFoundException("No se pudo crear el binding: la cola o el exchange no existen");
    }
    log.info("Binding creado: {} --[{}]--> {}", req.exchange(), req.routingKey(), req.queue());
  }

  public void eliminarBinding(BindingRequest req) {
    try {
      amqpAdmin.removeBinding(aBinding(req));
    } catch (AmqpIOException ex) {
      throw new ResourceNotFoundException("No se pudo eliminar el binding: la cola o el exchange no existen");
    }
    log.info("Binding eliminado: {} --[{}]--> {}", req.exchange(), req.routingKey(), req.queue());
  }

  // ---------- Mensajes de prueba ----------

  /** Publica un JSON a mano; útil para demostrar el consumo y el envío a la DLQ. */
  public void publicarPrueba(PublishRequest req) {
    MessageProperties props = new MessageProperties();
    props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
    props.setContentEncoding(StandardCharsets.UTF_8.name());
    rabbitTemplate.send(req.exchange(), req.routingKey(), new Message(req.payload().getBytes(StandardCharsets.UTF_8), props));
    log.info("Mensaje de prueba publicado en '{}' con routing key '{}'", req.exchange(), req.routingKey());
  }

  // ---------- Utilidades ----------

  private static Binding aBinding(BindingRequest req) {
    return new Binding(req.queue(), Binding.DestinationType.QUEUE, req.exchange(), req.routingKey(), null);
  }

  /** Los nombres que empiezan con "amq." son del propio RabbitMQ y no se pueden crear ni borrar. */
  private static void validarNombreNoReservado(String nombre) {
    if (nombre == null || nombre.isBlank()) {
      throw new BusinessException(HttpStatus.BAD_REQUEST, "El nombre no puede estar vacío");
    }
    if (nombre.startsWith("amq.")) {
      throw new BusinessException(HttpStatus.BAD_REQUEST, "El prefijo 'amq.' está reservado por RabbitMQ");
    }
  }

  private static int numero(Object valor) {
    return valor instanceof Number n ? n.intValue() : 0;
  }
}
