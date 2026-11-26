package cl.duoc.pedidos360.streaming.config;

import cl.duoc.pedidos360.streaming.event.InventarioEventoDto;
import cl.duoc.pedidos360.streaming.event.PedidoEventoDto;
import cl.duoc.pedidos360.streaming.support.EventoInvalidoException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * Configuración COMPLETA del cliente Kafka de ms-productos, en un solo lugar:
 * <ul>
 *   <li>Productor del tópico de inventario (serializers, acks, idempotencia).</li>
 *   <li>Consumidor del tópico de pedidos (deserializers, consumer group, offsets, polling).</li>
 *   <li>Manejo de errores: reintentos con espera exponencial y Dead Letter Topic (DLT).</li>
 * </ul>
 * Todos los valores salen de application.yml (pedidos360.kafka.*).
 */
@Configuration
public class KafkaClientConfig {

  private static final Logger log = LoggerFactory.getLogger(KafkaClientConfig.class);

  // ------------------------------------------------------------------ Productor (inventario)

  @Bean
  public ProducerFactory<String, InventarioEventoDto> inventarioProducerFactory(KafkaSettings s, ObjectMapper objectMapper) {
    JsonSerializer<InventarioEventoDto> valor = new JsonSerializer<>(objectMapper);
    valor.setAddTypeInfo(false);
    return new DefaultKafkaProducerFactory<>(propiedadesProductor(s, "ms-productos-producer"), new StringSerializer(), valor);
  }

  @Bean
  public KafkaTemplate<String, InventarioEventoDto> inventarioKafkaTemplate(
      ProducerFactory<String, InventarioEventoDto> inventarioProducerFactory) {
    return new KafkaTemplate<>(inventarioProducerFactory);
  }

  // ------------------------------------------------------------------ Consumidor (pedidos)

  /**
   * Fábrica de contenedores para los listeners del tópico de pedidos.
   * El valor se deserializa a {@link PedidoEventoDto}; {@link ErrorHandlingDeserializer} evita que un JSON roto
   * detenga el consumo (el mensaje se envía a la DLT en vez de reintentarse para siempre).
   */
  @Bean("pedidosKafkaListenerFactory")
  public ConcurrentKafkaListenerContainerFactory<String, PedidoEventoDto> pedidosKafkaListenerFactory(
      KafkaSettings s, ObjectMapper objectMapper, DefaultErrorHandler kafkaErrorHandler) {
    ErrorHandlingDeserializer<PedidoEventoDto> valor =
        new ErrorHandlingDeserializer<>(new JsonDeserializer<PedidoEventoDto>(PedidoEventoDto.class, objectMapper, false));
    DefaultKafkaConsumerFactory<String, PedidoEventoDto> consumerFactory =
        new DefaultKafkaConsumerFactory<>(propiedadesConsumidor(s, "ms-productos-consumer"), new StringDeserializer(), valor);

    ConcurrentKafkaListenerContainerFactory<String, PedidoEventoDto> fabrica = new ConcurrentKafkaListenerContainerFactory<>();
    fabrica.setConsumerFactory(consumerFactory);
    fabrica.setCommonErrorHandler(kafkaErrorHandler);
    fabrica.setConcurrency(s.consumer().concurrency());               // un hilo por partición
    fabrica.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);  // commit tras procesar cada mensaje
    return fabrica;
  }

  // ------------------------------------------------------------------ Manejo de errores

  /**
   * Política de errores común a los consumidores:
   * <ol>
   *   <li>Error transitorio: reintenta con espera exponencial (maxAttempts veces) y deja cada intento en el log.</li>
   *   <li>Mensaje inválido ({@link EventoInvalidoException}) o JSON ilegible: no se reintenta.</li>
   *   <li>Agotados los reintentos: se publica en {@code <tópico>.DLT} con la causa en las cabeceras y el consumo continúa.</li>
   * </ol>
   */
  @Bean
  public DefaultErrorHandler kafkaErrorHandler(KafkaSettings s, ObjectMapper objectMapper) {
    // Plantilla para mensajes que no se pudieron deserializar (se reenvían tal cual, en bytes)
    KafkaTemplate<String, byte[]> plantillaBytes = new KafkaTemplate<>(
        new DefaultKafkaProducerFactory<>(propiedadesProductor(s, "ms-productos-dlt-bytes"),
            new StringSerializer(), new ByteArraySerializer()));
    // Plantilla para eventos válidos que fallaron al procesarse (se reenvían como JSON)
    JsonSerializer<Object> json = new JsonSerializer<>(objectMapper);
    json.setAddTypeInfo(false);
    KafkaTemplate<String, Object> plantillaJson = new KafkaTemplate<>(
        new DefaultKafkaProducerFactory<>(propiedadesProductor(s, "ms-productos-dlt-json"), new StringSerializer(), json));

    Map<Class<?>, KafkaOperations<?, ?>> plantillas = new LinkedHashMap<>();
    plantillas.put(byte[].class, plantillaBytes);
    plantillas.put(Object.class, plantillaJson);

    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(plantillas,
        (registro, error) -> new TopicPartition(registro.topic() + s.topics().dltSuffix(), -1));

    ExponentialBackOffWithMaxRetries espera = new ExponentialBackOffWithMaxRetries(s.retry().maxAttempts());
    espera.setInitialInterval(s.retry().initialIntervalMs());
    espera.setMultiplier(s.retry().multiplier());
    espera.setMaxInterval(s.retry().maxIntervalMs());

    DefaultErrorHandler manejador = new DefaultErrorHandler(recoverer, espera);
    manejador.addNotRetryableExceptions(EventoInvalidoException.class);
    manejador.setRetryListeners(new RetryListener() {
      @Override
      public void failedDelivery(ConsumerRecord<?, ?> registro, Exception error, int intento) {
        log.warn("[KAFKA] Falló el procesamiento de {}-{}@{} (intento {}): {}",
            registro.topic(), registro.partition(), registro.offset(), intento, error.getMessage());
      }

      @Override
      public void recovered(ConsumerRecord<?, ?> registro, Exception error) {
        log.error("[KAFKA] Mensaje {}-{}@{} enviado a la DLT '{}{}': {}",
            registro.topic(), registro.partition(), registro.offset(),
            registro.topic(), s.topics().dltSuffix(), error.getMessage());
      }
    });
    return manejador;
  }

  // ------------------------------------------------------------------ Propiedades base

  private static Map<String, Object> propiedadesProductor(KafkaSettings s, String clientId) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, s.bootstrapServers());
    cfg.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
    cfg.put(ProducerConfig.ACKS_CONFIG, s.producer().acks());
    cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, s.producer().idempotence());
    cfg.put(ProducerConfig.RETRIES_CONFIG, s.producer().retries());
    cfg.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, s.producer().maxBlockMs());
    cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, s.producer().requestTimeoutMs());
    cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, s.producer().deliveryTimeoutMs());
    cfg.put(ProducerConfig.LINGER_MS_CONFIG, s.producer().lingerMs());
    cfg.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, s.producer().compression());
    return cfg;
  }

  /** El group.id NO va aquí: lo define cada @KafkaListener con su propio consumer group. */
  private static Map<String, Object> propiedadesConsumidor(KafkaSettings s, String clientId) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, s.bootstrapServers());
    cfg.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, s.consumer().autoOffsetReset());
    cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);          // el commit lo hace el contenedor tras procesar
    cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, s.consumer().maxPollRecords());
    cfg.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, s.consumer().maxPollIntervalMs());
    cfg.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, s.consumer().sessionTimeoutMs());
    cfg.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, s.consumer().heartbeatIntervalMs());
    cfg.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
    return cfg;
  }
}
