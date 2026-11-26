package cl.duoc.pedidos360.analitica;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AnaliticaConfig {

  /** Único almacén de indicadores: lo escriben los consumidores y lo lee el controlador. */
  @Bean
  public AnaliticaStore analiticaStore() {
    return new AnaliticaStore();
  }
}
