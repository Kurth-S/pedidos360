package cl.duoc.pedidos360.kafkaadmin;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import cl.duoc.pedidos360.common.BusinessException;
import cl.duoc.pedidos360.kafkaadmin.support.TopicConfigRules;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TopicConfigRulesTest {

  @Test
  void aceptaParametrosDeAdministracionValidos() {
    assertThatCode(() -> TopicConfigRules.validar(Map.of(
        "retention.ms", "604800000",
        "cleanup.policy", "compact,delete",
        "min.insync.replicas", "2"))).doesNotThrowAnyException();
  }

  @Test
  void rechazaConfiguracionDesconocida() {
    assertThatThrownBy(() -> TopicConfigRules.validar(Map.of("auto.create.topics.enable", "true")))
        .isInstanceOf(BusinessException.class)
        .hasMessageContaining("no permitida");
  }

  @Test
  void rechazaValoresNoNumericosYPoliticasInvalidas() {
    assertThatThrownBy(() -> TopicConfigRules.validar(Map.of("retention.ms", "siete dias")))
        .isInstanceOf(BusinessException.class);
    assertThatThrownBy(() -> TopicConfigRules.validar(Map.of("cleanup.policy", "borrar")))
        .isInstanceOf(BusinessException.class);
  }
}
