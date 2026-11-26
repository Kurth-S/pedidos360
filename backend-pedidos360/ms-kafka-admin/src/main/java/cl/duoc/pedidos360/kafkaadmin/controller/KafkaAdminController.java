package cl.duoc.pedidos360.kafkaadmin.controller;

import cl.duoc.pedidos360.kafkaadmin.dto.ClusterInfo;
import cl.duoc.pedidos360.kafkaadmin.dto.ConfigUpdateRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.GroupDetail;
import cl.duoc.pedidos360.kafkaadmin.dto.GroupSummary;
import cl.duoc.pedidos360.kafkaadmin.dto.PartitionsRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.PublishRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.PublishResult;
import cl.duoc.pedidos360.kafkaadmin.dto.ResetOffsetsRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicDetail;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicSummary;
import cl.duoc.pedidos360.kafkaadmin.service.ConsumerGroupAdminService;
import cl.duoc.pedidos360.kafkaadmin.service.MensajePruebaService;
import cl.duoc.pedidos360.kafkaadmin.service.TopicAdminService;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * API REST de administración de Kafka. Exige JWT válido (SecurityConfig) y rol Admin en todas las rutas.
 *
 * <pre>
 *  GET    /api/kafka/cluster                         brokers y controlador
 *  GET    /api/kafka/topics                          lista tópicos
 *  GET    /api/kafka/topics/{name}                   particiones (líder, réplicas, ISR) y configuración
 *  POST   /api/kafka/topics                          crea un tópico                       → 201
 *  PATCH  /api/kafka/topics/{name}/configs           fija retention.ms, cleanup.policy... → 204
 *  DELETE /api/kafka/topics/{name}/configs/{key}     restablece una configuración         → 204
 *  PATCH  /api/kafka/topics/{name}/partitions        aumenta las particiones              → 204
 *  DELETE /api/kafka/topics/{name}                   elimina un tópico                    → 204
 *  GET    /api/kafka/groups                          lista consumer groups
 *  GET    /api/kafka/groups/{id}                     miembros, offsets y lag
 *  POST   /api/kafka/groups/{id}/reset-offsets       reinicia offsets (grupo inactivo)    → 204
 *  DELETE /api/kafka/groups/{id}                     elimina un grupo inactivo            → 204
 *  POST   /api/kafka/messages                        publica un mensaje de prueba         → 202
 * </pre>
 */
@RestController
@RequestMapping("/api/kafka")
@PreAuthorize("hasRole('ADMIN')")
public class KafkaAdminController {

  private final TopicAdminService topicos;
  private final ConsumerGroupAdminService grupos;
  private final MensajePruebaService mensajes;

  public KafkaAdminController(TopicAdminService topicos, ConsumerGroupAdminService grupos, MensajePruebaService mensajes) {
    this.topicos = topicos;
    this.grupos = grupos;
    this.mensajes = mensajes;
  }

  @GetMapping("/cluster")
  public ClusterInfo cluster() {
    return topicos.cluster();
  }

  @GetMapping("/topics")
  public List<TopicSummary> listarTopicos() {
    return topicos.listar();
  }

  @GetMapping("/topics/{name}")
  public TopicDetail detalleTopico(@PathVariable String name) {
    return topicos.detalle(name);
  }

  @PostMapping("/topics")
  public ResponseEntity<Void> crearTopico(@Valid @RequestBody TopicRequest req) {
    topicos.crear(req);
    return ResponseEntity.created(URI.create("/api/kafka/topics/" + req.name())).build();
  }

  @PatchMapping("/topics/{name}/configs")
  public ResponseEntity<Void> modificarConfiguracion(@PathVariable String name, @Valid @RequestBody ConfigUpdateRequest req) {
    topicos.modificarConfiguracion(name, req.configs());
    return ResponseEntity.noContent().build();
  }

  @DeleteMapping("/topics/{name}/configs/{key}")
  public ResponseEntity<Void> restablecerConfiguracion(@PathVariable String name, @PathVariable String key) {
    topicos.restablecerConfiguracion(name, key);
    return ResponseEntity.noContent().build();
  }

  @PatchMapping("/topics/{name}/partitions")
  public ResponseEntity<Void> aumentarParticiones(@PathVariable String name, @Valid @RequestBody PartitionsRequest req) {
    topicos.aumentarParticiones(name, req.partitions());
    return ResponseEntity.noContent().build();
  }

  @DeleteMapping("/topics/{name}")
  public ResponseEntity<Void> eliminarTopico(@PathVariable String name) {
    topicos.eliminar(name);
    return ResponseEntity.noContent().build();
  }

  @GetMapping("/groups")
  public List<GroupSummary> listarGrupos() {
    return grupos.listar();
  }

  @GetMapping("/groups/{id}")
  public GroupDetail detalleGrupo(@PathVariable String id) {
    return grupos.detalle(id);
  }

  @PostMapping("/groups/{id}/reset-offsets")
  public ResponseEntity<Void> reiniciarOffsets(@PathVariable String id, @Valid @RequestBody ResetOffsetsRequest req) {
    grupos.reiniciarOffsets(id, req);
    return ResponseEntity.noContent().build();
  }

  @DeleteMapping("/groups/{id}")
  public ResponseEntity<Void> eliminarGrupo(@PathVariable String id) {
    grupos.eliminar(id);
    return ResponseEntity.noContent().build();
  }

  @PostMapping("/messages")
  public ResponseEntity<PublishResult> publicarPrueba(@Valid @RequestBody PublishRequest req) {
    return ResponseEntity.accepted().body(mensajes.publicar(req));
  }
}
