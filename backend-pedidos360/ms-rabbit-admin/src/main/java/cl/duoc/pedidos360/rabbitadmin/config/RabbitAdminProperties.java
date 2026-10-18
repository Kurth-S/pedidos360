package cl.duoc.pedidos360.rabbitadmin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** URL de la API HTTP del plugin de administración de RabbitMQ. */
@ConfigurationProperties(prefix = "pedidos360.rabbit-admin")
public record RabbitAdminProperties(String managementUrl) {

  public RabbitAdminProperties {
    if (managementUrl == null || managementUrl.isBlank()) {
      managementUrl = "http://localhost:15672";
    }
  }
}
