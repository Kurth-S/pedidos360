package cl.duoc.pedidos360.kafkaadmin.support;

import cl.duoc.pedidos360.common.BusinessException;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.springframework.http.HttpStatus;

/** Reglas de validación de la configuración de un tópico (solo parámetros de administración conocidos). */
public final class TopicConfigRules {

  /** Parámetros que la API permite fijar. */
  public static final Set<String> PERMITIDAS = Set.of(
      "retention.ms", "retention.bytes", "cleanup.policy", "min.insync.replicas", "segment.ms",
      "segment.bytes", "max.message.bytes", "compression.type", "delete.retention.ms");

  private static final Set<String> NUMERICAS = Set.of(
      "retention.ms", "retention.bytes", "min.insync.replicas", "segment.ms", "segment.bytes",
      "max.message.bytes", "delete.retention.ms");

  private static final Set<String> POLITICAS = Set.of("delete", "compact", "compact,delete", "delete,compact");

  private TopicConfigRules() {}

  public static void validar(Map<String, String> configs) {
    if (configs == null) {
      return;
    }
    for (Map.Entry<String, String> e : configs.entrySet()) {
      validarClave(e.getKey());
      String valor = e.getValue() == null ? "" : e.getValue().trim();
      if (valor.isEmpty()) {
        throw new BusinessException(HttpStatus.BAD_REQUEST, "La configuración '" + e.getKey() + "' no puede estar vacía");
      }
      if (NUMERICAS.contains(e.getKey())) {
        try {
          // -1 significa "sin límite" en retention.ms y retention.bytes
          if (Long.parseLong(valor) < -1) {
            throw new NumberFormatException();
          }
        } catch (NumberFormatException ex) {
          throw new BusinessException(HttpStatus.BAD_REQUEST,
              "La configuración '" + e.getKey() + "' debe ser un número entero (recibido: " + valor + ")");
        }
      }
      if ("cleanup.policy".equals(e.getKey()) && !POLITICAS.contains(valor)) {
        throw new BusinessException(HttpStatus.BAD_REQUEST,
            "cleanup.policy debe ser uno de: delete, compact, compact,delete (recibido: " + valor + ")");
      }
    }
  }

  public static void validarClave(String clave) {
    if (!PERMITIDAS.contains(clave)) {
      throw new BusinessException(HttpStatus.BAD_REQUEST,
          "Configuración no permitida: '" + clave + "'. Permitidas: " + new TreeSet<>(PERMITIDAS));
    }
  }
}
