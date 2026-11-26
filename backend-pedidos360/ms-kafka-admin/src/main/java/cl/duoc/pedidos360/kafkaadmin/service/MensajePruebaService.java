package cl.duoc.pedidos360.kafkaadmin.service;

import cl.duoc.pedidos360.common.BusinessException;
import cl.duoc.pedidos360.kafkaadmin.config.KafkaAdminProperties;
import cl.duoc.pedidos360.kafkaadmin.dto.PublishRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.PublishResult;
import cl.duoc.pedidos360.kafkaadmin.support.KafkaFutures;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/** Publica un mensaje de texto en un tópico para probar los consumidores (incluido el camino a la DLT). */
@Service
public class MensajePruebaService {

  private static final Logger log = LoggerFactory.getLogger(MensajePruebaService.class);

  private final KafkaProducer<String, String> producer;
  private final int timeout;

  public MensajePruebaService(KafkaProducer<String, String> producer, KafkaAdminProperties props) {
    this.producer = producer;
    this.timeout = props.requestTimeoutMs() * 2;
  }

  public PublishResult publicar(PublishRequest req) {
    try {
      RecordMetadata meta = producer.send(new ProducerRecord<>(req.topic(), req.key(), req.payload()))
          .get(timeout, TimeUnit.MILLISECONDS);
      log.info("[KAFKA-ADMIN] Mensaje de prueba publicado en {} (partición {}, offset {})",
          meta.topic(), meta.partition(), meta.offset());
      return new PublishResult(meta.topic(), meta.partition(), meta.offset());
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new BusinessException(HttpStatus.BAD_GATEWAY, "La publicación fue interrumpida");
    } catch (java.util.concurrent.TimeoutException ex) {
      throw new BusinessException(HttpStatus.GATEWAY_TIMEOUT, "Kafka no confirmó el mensaje en " + timeout + " ms");
    } catch (ExecutionException ex) {
      throw KafkaFutures.traducir(ex.getCause() != null ? ex.getCause() : ex, req.topic());
    } catch (KafkaException ex) {
      throw KafkaFutures.traducir(ex, req.topic());
    }
  }
}
