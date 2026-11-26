package cl.duoc.pedidos360.streaming.support;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/** Valida el payload de un evento con Bean Validation; si falla lanza {@link EventoInvalidoException} (no reintentable). */
@Component
public class ValidadorEventos {

  private final Validator validator;

  public ValidadorEventos(Validator validator) {
    this.validator = validator;
  }

  public <T> T validar(T evento, String descripcion) {
    if (evento == null) {
      throw new EventoInvalidoException(descripcion + ": mensaje sin contenido");
    }
    Set<ConstraintViolation<T>> errores = validator.validate(evento);
    if (!errores.isEmpty()) {
      String detalle = errores.stream()
          .map(e -> e.getPropertyPath() + " " + e.getMessage())
          .sorted()
          .collect(Collectors.joining("; "));
      throw new EventoInvalidoException(descripcion + " inválido: " + detalle);
    }
    return evento;
  }
}
