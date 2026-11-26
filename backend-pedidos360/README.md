# Pedidos360 · Backend (Spring Boot 3.5 · Java 21)

Seis microservicios Spring Boot (más RabbitMQ para tareas asíncronas y Kafka para streaming de eventos). Cada uno **valida el JWT del IDaaS** (firma vía JWKS, vigencia, issuer, audience)
con Spring Security OAuth2 Resource Server y autoriza por **rol** (claim `roles` → `ROLE_*`) y **scope**
(claim `scp` → `SCOPE_*`). Se despliegan en **EC2** y persisten en una base de datos cloud (**RDS MySQL**).

| Servicio | Puerto | Rutas | Autorización | BD |
|---|---|---|---|---|
| `bff-pedidos360` | 8080 | `GET /api/bff/me` · `GET /api/bff/dashboard` · `GET /api/bff/admin/pedidos` | JWT válido · scope Read · rol Admin | — (compone ms-productos y ms-pedidos reenviando el JWT) |
| `ms-productos` | 8081 | `GET /api/productos` · `GET /api/productos/{id}` · `POST /api/productos` | scope Read · scope Read · rol Admin | `pedidos360_productos` (entidad `Producto`) |
| `ms-rabbit-admin` | 8083 | `GET/POST /api/rabbit/queues` · `DELETE /api/rabbit/queues/{name}` · `POST /api/rabbit/exchanges` · `DELETE /api/rabbit/exchanges/{name}` · `POST/DELETE /api/rabbit/bindings` · `POST /api/rabbit/messages` | JWT válido · rol Admin | — (administra el broker RabbitMQ) |
| `ms-kafka-admin` | 8084 | `GET /api/kafka/cluster` · `GET/POST /api/kafka/topics` · `GET/DELETE /api/kafka/topics/{name}` · `PATCH /api/kafka/topics/{name}/configs` · `DELETE /api/kafka/topics/{name}/configs/{key}` · `PATCH /api/kafka/topics/{name}/partitions` · `GET /api/kafka/groups` · `GET/DELETE /api/kafka/groups/{id}` · `POST /api/kafka/groups/{id}/reset-offsets` · `POST /api/kafka/messages` | JWT válido · rol Admin | — (administra el clúster Kafka con `AdminClient`) |
| `ms-analitica` | 8085 | `GET /api/analitica/resumen?top=5` | JWT válido · rol Admin | — (indicadores en memoria, reconstruidos desde Kafka) |
| `ms-pedidos` | 8082 | `POST /api/pedidos` · `GET /api/pedidos` · `GET /api/pedidos/{id}` | scope Write · scope Read (Cliente ve solo los suyos) · dueño o Admin | `pedidos360_pedidos` (entidades `Pedido`, `PedidoItem`) |

Códigos de respuesta: `401` sin token o token inválido (firma, exp, iss, aud) · `403` sin el rol/scope o pedido de
otro usuario · `404` no existe · `400` validación · `409` producto no disponible.

Paquetes comunes (`cl.duoc.pedidos360.security`, `cl.duoc.pedidos360.common`): `SecurityConfig` (`NimbusJwtDecoder` +
`JwtTimestampValidator` + `JwtIssuerValidator` + `AudienceValidator`), `ClaimsAuthoritiesConverter`,
`JsonAuthenticationEntryPoint` (401), `JsonAccessDeniedHandler` (403), `GlobalExceptionHandler`.

## Configuración (variables de entorno o `application.yml`)

| Variable | Valor |
|---|---|
| `JWT_ISSUER` | claim `iss` del token: `https://<tenant-id>.ciamlogin.com/<tenant-id>/v2.0` |
| `JWT_JWKS_URI` | `https://<subdominio>.ciamlogin.com/<tenant-id>/discovery/v2.0/keys` |
| `JWT_AUDIENCE` | client ID de la app **Pedidos360-API** |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:4200,https://<url-del-frontend>` |
| `DB_URL`, `DB_USER`, `DB_PASSWORD` | conexión a RDS MySQL (una por microservicio; ver `application.yml`) |
| `PRODUCTOS_URL`, `PEDIDOS_URL` | URLs internas entre servicios (en EC2: `http://localhost:8081` / `8082`) |
| `KAFKA_BOOTSTRAP_SERVERS` | brokers Kafka: `localhost:9092,localhost:9093,localhost:9094` (ms-pedidos, ms-productos, ms-kafka-admin, ms-analitica) |

## Ejecutar / desplegar en EC2

```bash
# Amazon Linux 2023: sudo dnf install -y java-21-amazon-corretto maven git
mvn -f ms-productos/pom.xml   -DskipTests package && nohup java -jar ms-productos/target/ms-productos-1.0.0.jar &
mvn -f ms-pedidos/pom.xml     -DskipTests package && nohup java -jar ms-pedidos/target/ms-pedidos-1.0.0.jar &
mvn -f bff-pedidos360/pom.xml -DskipTests package && nohup java -jar bff-pedidos360/target/bff-pedidos360-1.0.0.jar &
```
Los servicios de RabbitMQ y Kafka se compilan igual (`ms-rabbit-admin`, `ms-kafka-admin`, `ms-analitica`; ver sus secciones).
Exportar antes las variables de la tabla. Security group de la EC2: puertos 8080-8085 abiertos para el API Gateway.

## Streaming de eventos con Kafka (EP5)

Kafka se agrega **junto a** RabbitMQ, sin tocar su código ni sus colas:

| | RabbitMQ (EP3) | Kafka (EP5) |
|---|---|---|
| Para qué | **Tareas**: notificar, generar ticket, revisar inventario | **Registro de hechos** para analítica |
| Un mensaje | lo procesa UNA cola y se elimina al confirmarse | queda retenido (7 días) y lo lee cada consumer group por separado |
| Nuevo consumidor | hay que crear cola y binding en el productor | basta con un `groupId` nuevo; el productor no cambia |
| Fallos | DLQ (`*.dlq`) | DLT (`<tópico>.DLT`) |

```
                      ┌─ RabbitMQ (EP3) ─ notificaciones · tickets · inventario ─ DLQ
POST /api/pedidos ────┤
   (ms-pedidos)       └─ Kafka   ─ pedidos360.pedidos.eventos ─┬─ grupo productos.inventario-demanda (ms-productos)
                                                                │        └─ publica ─► pedidos360.inventario.eventos
                                                                └─ grupo analitica.ventas (ms-analitica)
                                   pedidos360.inventario.eventos ──── grupo analitica.inventario (ms-analitica)
                                   *.DLT ◄── mensajes inválidos o que agotaron los reintentos
```

### Tópicos (los crea `ms-kafka-admin` al iniciar, desde `application.yml`)

| Tópico | Particiones | Réplicas | Clave | `cleanup.policy` | `retention.ms` | `min.insync.replicas` |
|---|---|---|---|---|---|---|
| `pedidos360.pedidos.eventos` | 3 | 3 | id del pedido | `delete` | 7 días | 2 |
| `pedidos360.inventario.eventos` | 3 | 3 | id del producto | `compact,delete` | 3 días | 2 |
| `pedidos360.pedidos.eventos.DLT` | 1 | 3 | — | `delete` | 14 días | 2 |
| `pedidos360.inventario.eventos.DLT` | 1 | 3 | — | `delete` | 14 días | 2 |

*Por qué cada política:* los pedidos son **hechos** (un registro por evento, no se compactan). El inventario es **estado**
(importa el último stock de cada producto), por eso se compacta por clave. Las DLT se guardan 14 días para revisarlas.
Si el clúster tiene menos brokers que el factor pedido (p. ej. un broker en desarrollo), el administrador usa los
disponibles y lo deja en el log.

### Quién produce y quién consume

| Servicio | Produce | Consume (`consumer group`) |
|---|---|---|
| `ms-pedidos` | `pedidos360.pedidos.eventos` (`PedidoKafkaPublisher`, tras el commit del pedido) | — |
| `ms-productos` | `pedidos360.inventario.eventos` (`InventarioKafkaPublisher`) | `pedidos360.pedidos.eventos` → `productos.inventario-demanda` (`PedidoKafkaConsumer`) |
| `ms-analitica` | — | `pedidos360.pedidos.eventos` → `analitica.ventas` · `pedidos360.inventario.eventos` → `analitica.inventario` |
| `ms-kafka-admin` | mensajes de prueba (`POST /api/kafka/messages`) | — |

Los eventos son DTO tipados (`PedidoEventoDto`, `InventarioEventoDto`) serializados como JSON con validación Bean Validation.
Cada servicio concentra su configuración en `streaming/config/KafkaClientConfig` + `KafkaSettings` (valores en `application.yml`):
brokers, serializers/deserializers, `acks=all`, idempotencia, `group.id`, `auto.offset.reset`, `max.poll.*`, sesión/heartbeat,
estrategia `CooperativeStickyAssignor`, concurrencia (3 hilos = 3 particiones) y política de reintentos.

### Manejo de errores

1. **Productor**: `acks=all` + idempotencia + `max.block.ms` bajo. Si Kafka cae, el pedido igualmente se crea y el error queda en el log.
2. **Consumidor**: valida el payload (`ValidadorEventos`). Un JSON ilegible (`ErrorHandlingDeserializer`) o un evento inválido
   (`EventoInvalidoException`) **no se reintenta** y va directo a `<tópico>.DLT`.
3. Un error transitorio se **reintenta 3 veces con espera exponencial** (500 ms → 1 s → 2 s) registrando cada intento; si se agota, va a la DLT
   con la causa en las cabeceras y el consumo **continúa** con el siguiente mensaje.
4. `ms-analitica` es **idempotente** por `eventId`: un duplicado (reintento, replay) no se vuelve a sumar.

### Cómo levantarlo

```bash
cp .env.example .env            # completa KAFKA_UI_PASSWORD (y las claves de RabbitMQ)
docker compose -f docker-compose.kafka.yml up -d        # Zookeeper + 3 brokers
docker compose -f docker-compose.kafka-ui.yml up -d     # Kafka UI en http://localhost:8090
# servicios (cada uno con sus variables JWT_* y KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9093,localhost:9094)
java -jar ms-kafka-admin/target/ms-kafka-admin-1.0.0.jar   # crea los 4 tópicos
java -jar ms-productos/target/ms-productos-1.0.0.jar
java -jar ms-pedidos/target/ms-pedidos-1.0.0.jar
java -jar ms-analitica/target/ms-analitica-1.0.0.jar
```

Pruebas rápidas (con `TOKEN` de un usuario Admin): crear un pedido desde la app o `POST /api/pedidos`, luego

```bash
curl -s -H "Authorization: Bearer $TOKEN" localhost:8084/api/kafka/topics
curl -s -H "Authorization: Bearer $TOKEN" localhost:8084/api/kafka/groups/analitica.ventas      # miembros, offsets y lag
curl -s -H "Authorization: Bearer $TOKEN" localhost:8085/api/analitica/resumen                  # ventas, top productos, stock bajo
# mensaje roto → debe terminar en pedidos360.pedidos.eventos.DLT
curl -s -X POST localhost:8084/api/kafka/messages -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"topic":"pedidos360.pedidos.eventos","key":"999","payload":"{esto no es json"}'
```

## API Gateway (HTTP API) — rutas, authorizer y CORS

Integraciones HTTP hacia `http://<ip-ec2>:8080|8081|8082` y rutas explícitas:

| Método | Ruta | Backend | Scope en el authorizer |
|---|---|---|---|
| GET | `/api/bff/me` | 8080 | (solo token válido) |
| GET | `/api/bff/dashboard` | 8080 | `Pedidos360.Read` |
| GET | `/api/bff/admin/pedidos` | 8080 | `Pedidos360.Read` (el rol Admin lo valida el BFF) |
| GET / POST | `/api/productos` | 8081 | `Pedidos360.Read` / `Pedidos360.Write` |
| GET | `/api/productos/{id}` | 8081 | `Pedidos360.Read` |
| GET / POST | `/api/pedidos` | 8082 | `Pedidos360.Read` / `Pedidos360.Write` |
| GET | `/api/pedidos/{id}` | 8082 | `Pedidos360.Read` |
| ANY | `/api/kafka/{proxy+}` | 8084 | (token válido; el rol Admin lo valida `ms-kafka-admin`) |
| GET | `/api/analitica/resumen` | 8085 | (token válido; el rol Admin lo valida `ms-analitica`) |

- **Authorizer JWT**: identity source `$request.header.Authorization`, issuer = `JWT_ISSUER`, audience = `JWT_AUDIENCE`;
  adjunto a todas las rutas. Sin token / token inválido → 401; sin el scope → 403.
- **CORS**: orígenes `https://<url-del-frontend>` y `http://localhost:4200`; métodos `GET, POST, OPTIONS`;
  headers `Authorization, Content-Type`.

## Mensajería asíncrona con RabbitMQ

Al crear un pedido, `ms-pedidos` publica el evento `pedido.creado` **después del commit** y responde de inmediato.
Si RabbitMQ no está disponible, el pedido igualmente se guarda (el error queda en el log). Los consumidores
procesan el evento por separado:

```
 ms-pedidos ──pedido.creado──► pedidos360.eventos (topic)
                                 ├─► notificaciones.pedido-creado.q ─► NotificacionPedidoConsumer  (ms-pedidos)  correo al cliente
                                 ├─► tickets.pedido-creado.q ────────► TicketPedidoConsumer        (ms-pedidos)  ticket del pedido
                                 └─► inventario.pedido-creado.q ─────► InventarioPedidoConsumer    (ms-productos) revisa stock
 ms-productos ─inventario.stock-bajo─► pedidos360.eventos ─► notificaciones.stock-bajo.q ─► AlertaStockConsumer (ms-pedidos) alerta al admin
 Cada cola tiene su DLQ (<nombre>.dlq) enlazada al exchange pedidos360.eventos.dlx.
```

| Pieza | Dónde |
|---|---|
| Nombres de exchanges, colas y routing keys | `application.yml` → `pedidos360.rabbit` (único lugar) |
| Beans `Exchange` / `Queue` / `Binding` | `messaging/config/RabbitTopologyConfig` (solo infraestructura) |
| Productores | `messaging/producer/*Publisher` |
| Consumidores (`@RabbitListener`), agrupados por dominio | `messaging/notificaciones`, `messaging/tickets`, `messaging/alertas`, `messaging/inventario` |
| ACK manual y política de errores | `messaging/support/ConfirmacionMensajes` |
| Administración del broker | `ms-rabbit-admin`: `RabbitAdminController` → `RabbitAdminService` → `AmqpAdmin` |

**Política de confirmación (ACK):** tarea OK → `ACK`. `MensajeInvalidoException` (datos incorrectos) → `NACK` sin
reencolar: va a la **DLQ** y se registra con `log.error`. Cualquier otro error → un reintento (`NACK` con reencolar);
si vuelve a fallar → DLQ y `log.error`.

Variables nuevas: `RABBIT_HOST`, `RABBIT_PORT`, `RABBIT_USER`, `RABBIT_PASSWORD` (todos los servicios),
`RABBIT_MANAGEMENT_URL` (solo `ms-rabbit-admin`, para listar colas) y `STOCK_BAJO_UMBRAL` (`ms-productos`, por defecto 5).

### Broker en EC2 (Docker)

```bash
sudo dnf install -y docker && sudo systemctl enable --now docker
sudo docker run -d --name rabbitmq --restart unless-stopped -p 5672:5672 -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=pedidos360 -e RABBITMQ_DEFAULT_PASS=<clave> rabbitmq:3-management
```

No abrir 5672 ni 15672 en el security group: los servicios usan el broker por `localhost`.
