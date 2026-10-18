package cl.duoc.pedidos360.rabbitadmin.client;

import cl.duoc.pedidos360.rabbitadmin.config.RabbitAdminProperties;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/** Cliente de la API HTTP de RabbitMQ. AmqpAdmin no sabe listar colas; esta API sí. */
@Component
public class RabbitManagementClient {

  private final RestClient restClient;

  public RabbitManagementClient(RabbitAdminProperties props,
                                @Value("${spring.rabbitmq.username}") String user,
                                @Value("${spring.rabbitmq.password}") String password) {
    SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
    factory.setConnectTimeout(2000);
    factory.setReadTimeout(3000);
    this.restClient = RestClient.builder()
        .requestFactory(factory)
        .baseUrl(props.managementUrl())
        .defaultHeaders(h -> h.setBasicAuth(user, password))
        .build();
  }

  public List<Map<String, Object>> listarColas() {
    List<Map<String, Object>> colas = restClient.get()
        .uri("/api/queues")
        .retrieve()
        .body(new ParameterizedTypeReference<List<Map<String, Object>>>() {});
    return colas == null ? List.of() : colas;
  }
}
