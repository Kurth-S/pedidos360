package cl.duoc.pedidos360.kafkaadmin.support;

import cl.duoc.pedidos360.common.BusinessException;
import cl.duoc.pedidos360.common.ResourceNotFoundException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.http.HttpStatus;

/** Espera el resultado de una operación del AdminClient y traduce los errores de Kafka a códigos HTTP. */
public final class KafkaFutures {

  private KafkaFutures() {}

  public static <T> T esperar(KafkaFuture<T> future, int timeoutMs, String recurso) {
    try {
      return future.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new BusinessException(HttpStatus.BAD_GATEWAY, "La operación sobre Kafka fue interrumpida");
    } catch (java.util.concurrent.TimeoutException ex) {
      throw new BusinessException(HttpStatus.GATEWAY_TIMEOUT,
          "Kafka no respondió en " + timeoutMs + " ms (¿brokers caídos?)");
    } catch (ExecutionException ex) {
      throw traducir(ex.getCause() != null ? ex.getCause() : ex, recurso);
    }
  }

  public static RuntimeException traducir(Throwable causa, String recurso) {
    if (causa instanceof TopicExistsException) {
      return new BusinessException(HttpStatus.CONFLICT, "El tópico '" + recurso + "' ya existe");
    }
    if (causa instanceof UnknownTopicOrPartitionException) {
      return new ResourceNotFoundException("El tópico '" + recurso + "' no existe");
    }
    if (causa instanceof GroupIdNotFoundException) {
      return new ResourceNotFoundException("El consumer group '" + recurso + "' no existe");
    }
    if (causa instanceof GroupNotEmptyException) {
      return new BusinessException(HttpStatus.CONFLICT,
          "El consumer group '" + recurso + "' tiene consumidores activos: detén los servicios que lo usan");
    }
    if (causa instanceof InvalidTopicException
        || causa instanceof InvalidReplicationFactorException
        || causa instanceof InvalidPartitionsException
        || causa instanceof InvalidConfigurationException
        || causa instanceof InvalidRequestException) {
      return new BusinessException(HttpStatus.BAD_REQUEST, "Kafka rechazó la solicitud: " + causa.getMessage());
    }
    if (causa instanceof org.apache.kafka.common.errors.TimeoutException) {
      return new BusinessException(HttpStatus.GATEWAY_TIMEOUT, "Kafka no respondió a tiempo: " + causa.getMessage());
    }
    return new BusinessException(HttpStatus.BAD_GATEWAY, "Error de Kafka: " + causa.getMessage());
  }
}
