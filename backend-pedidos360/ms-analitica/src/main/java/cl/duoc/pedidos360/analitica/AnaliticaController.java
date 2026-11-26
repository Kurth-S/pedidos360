package cl.duoc.pedidos360.analitica;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Indicadores en tiempo real calculados desde los tópicos Kafka. Solo rol Admin (JWT validado en SecurityConfig). */
@RestController
@RequestMapping("/api/analitica")
@PreAuthorize("hasRole('ADMIN')")
public class AnaliticaController {

  private final AnaliticaStore store;

  public AnaliticaController(AnaliticaStore store) {
    this.store = store;
  }

  /** GET /api/analitica/resumen?top=5 */
  @GetMapping("/resumen")
  public ResumenAnalitico resumen(@RequestParam(defaultValue = "5") int top) {
    return store.resumen(Math.min(Math.max(top, 1), 50));
  }
}
