package cl.duoc.pedidos360.messaging.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Los mensajes viajan como JSON. El tipo se infiere del parámetro del método @RabbitListener,
 * así productor y consumidor (en microservicios distintos) no necesitan compartir clases.
 */
@Configuration
public class RabbitConverterConfig {

  @Bean
  MessageConverter jsonMessageConverter(ObjectMapper objectMapper) {
    Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter(objectMapper);
    DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
    typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.INFERRED);
    typeMapper.setTrustedPackages("*");
    converter.setJavaTypeMapper(typeMapper);
    return converter;
  }
}
