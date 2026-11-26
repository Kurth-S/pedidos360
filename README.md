# Pedidos360 — DSY1107 (EP1 encargo + EP2 presentación + EP3 RabbitMQ + EP5 Kafka)

Dos repositorios GitHub, uno por tecnología:

- `frontend-pedidos360/` → Angular + MSAL (login con Entra, guards, MsalInterceptor, consumo vía API Gateway).
- `backend-pedidos360/` → microservicios Spring Boot con filtros JWT, para EC2 + RDS:
  `bff-pedidos360`, `ms-productos`, `ms-pedidos`, `ms-rabbit-admin` (EP3), `ms-kafka-admin` y `ms-analitica` (EP5).

Infraestructura con Docker Compose (en la raíz):

| Archivo | Levanta |
|---|---|
| `docker-compose.yml` | clúster RabbitMQ de 2 nodos (`rabbit1`, `rabbit2`) |
| `docker-compose.kafka.yml` | Zookeeper + 3 brokers Kafka (replicación 3) |
| `docker-compose.kafka-ui.yml` | Kafka UI (administración de brokers, tópicos y consumer groups) |

Las variables sensibles van en `.env` (no se sube a GitHub); `.env.example` lista las que hacen falta.
Cada carpeta tiene su README con la configuración necesaria, la tabla de rutas/CORS del API Gateway y, en el backend,
la explicación de RabbitMQ (colas, exchanges, DLQ) y de Kafka (tópicos, consumer groups, DLT).
