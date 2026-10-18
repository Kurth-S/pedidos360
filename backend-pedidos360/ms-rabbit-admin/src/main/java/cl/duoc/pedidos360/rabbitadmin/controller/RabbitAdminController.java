package cl.duoc.pedidos360.rabbitadmin.controller;

import cl.duoc.pedidos360.rabbitadmin.dto.BindingRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.ExchangeRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.PublishRequest;
import cl.duoc.pedidos360.rabbitadmin.dto.QueueInfo;
import cl.duoc.pedidos360.rabbitadmin.dto.QueueRequest;
import cl.duoc.pedidos360.rabbitadmin.service.RabbitAdminService;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * API REST de administración de RabbitMQ. Exige JWT válido (SecurityConfig) y rol Admin en todas las rutas.
 *
 * <pre>
 *  GET    /api/rabbit/queues              lista colas
 *  POST   /api/rabbit/queues              crea una cola          → 201
 *  DELETE /api/rabbit/queues/{name}       elimina una cola       → 204
 *  POST   /api/rabbit/exchanges           crea un exchange       → 201
 *  DELETE /api/rabbit/exchanges/{name}    elimina un exchange    → 204
 *  POST   /api/rabbit/bindings            crea un binding        → 201
 *  DELETE /api/rabbit/bindings            elimina un binding     → 204 (cuerpo JSON)
 *  POST   /api/rabbit/messages            publica un JSON de prueba → 202
 * </pre>
 */
@RestController
@RequestMapping("/api/rabbit")
@PreAuthorize("hasRole('ADMIN')")
public class RabbitAdminController {

  private final RabbitAdminService service;

  public RabbitAdminController(RabbitAdminService service) {
    this.service = service;
  }

  @GetMapping("/queues")
  public List<QueueInfo> listarColas() {
    return service.listarColas();
  }

  @PostMapping("/queues")
  public ResponseEntity<Void> crearCola(@Valid @RequestBody QueueRequest req) {
    service.crearCola(req);
    return ResponseEntity.created(URI.create("/api/rabbit/queues/" + req.name())).build();
  }

  @DeleteMapping("/queues/{name}")
  public ResponseEntity<Void> eliminarCola(@PathVariable String name) {
    service.eliminarCola(name);
    return ResponseEntity.noContent().build();
  }

  @PostMapping("/exchanges")
  public ResponseEntity<Void> crearExchange(@Valid @RequestBody ExchangeRequest req) {
    service.crearExchange(req);
    return ResponseEntity.created(URI.create("/api/rabbit/exchanges/" + req.name())).build();
  }

  @DeleteMapping("/exchanges/{name}")
  public ResponseEntity<Void> eliminarExchange(@PathVariable String name) {
    service.eliminarExchange(name);
    return ResponseEntity.noContent().build();
  }

  @PostMapping("/bindings")
  public ResponseEntity<Void> crearBinding(@Valid @RequestBody BindingRequest req) {
    service.crearBinding(req);
    return ResponseEntity.status(201).build();
  }

  @DeleteMapping("/bindings")
  public ResponseEntity<Void> eliminarBinding(@Valid @RequestBody BindingRequest req) {
    service.eliminarBinding(req);
    return ResponseEntity.noContent().build();
  }

  @PostMapping("/messages")
  public ResponseEntity<Void> publicarPrueba(@Valid @RequestBody PublishRequest req) {
    service.publicarPrueba(req);
    return ResponseEntity.accepted().build();
  }
}
