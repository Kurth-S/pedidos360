package cl.duoc.pedidos360.messaging.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Regla de inventario: con stock menor o igual al umbral se publica una alerta de stock bajo. */
@ConfigurationProperties(prefix = "pedidos360.inventario")
public record InventarioProperties(Integer umbralStockBajo) {

  public InventarioProperties {
    if (umbralStockBajo == null || umbralStockBajo < 0) {
      umbralStockBajo = 5;
    }
  }
}
