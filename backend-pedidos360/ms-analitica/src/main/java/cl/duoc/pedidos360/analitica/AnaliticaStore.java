package cl.duoc.pedidos360.analitica;

import cl.duoc.pedidos360.streaming.event.InventarioEventoDto;
import cl.duoc.pedidos360.streaming.event.PedidoEventoDto;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Acumula los indicadores a partir de los eventos. Es idempotente: cada evento se identifica por su eventId y, si llega
 * dos veces (reintento del productor, reproceso al reconstruir desde el inicio del tópico), se descarta sin sumarlo.
 * Los métodos son synchronized: los consumidores de ambos tópicos escriben desde hilos distintos.
 */
public class AnaliticaStore {

  private static final int MAX_EVENTOS_RECORDADOS = 20_000;
  private static final int MAX_ALERTAS = 20;

  private final Set<String> vistos = new LinkedHashSet<>();

  private long totalPedidos;
  private BigDecimal ingresos = BigDecimal.ZERO;
  private long unidades;
  private final Set<String> clientes = new HashSet<>();
  private final Map<Long, Acumulado> porProducto = new HashMap<>();

  private final Map<Long, ResumenAnalitico.EstadoStock> stock = new HashMap<>();
  private final Deque<ResumenAnalitico.AlertaStock> alertas = new ArrayDeque<>();

  private long eventosPedidos;
  private long eventosInventario;
  private long duplicados;
  private Instant ultimoEvento;

  /** @return false si el evento ya se había procesado (duplicado) */
  public synchronized boolean registrarPedido(PedidoEventoDto e) {
    if (!recordar("P:" + e.eventId())) {
      return false;
    }
    eventosPedidos++;
    actualizarUltimo(e.ocurridoEn());
    totalPedidos++;
    ingresos = ingresos.add(e.total());
    clientes.add(e.clienteOid());
    for (PedidoEventoDto.Item item : e.items()) {
      unidades += item.cantidad();
      Acumulado a = porProducto.computeIfAbsent(item.productoId(), id -> new Acumulado());
      a.nombre = item.nombreProducto();
      a.unidades += item.cantidad();
      a.ingresos = a.ingresos.add(item.subtotal());
    }
    return true;
  }

  /** @return false si el evento ya se había procesado (duplicado) */
  public synchronized boolean registrarInventario(InventarioEventoDto e) {
    if (!recordar("I:" + e.eventId())) {
      return false;
    }
    eventosInventario++;
    actualizarUltimo(e.ocurridoEn());

    ResumenAnalitico.EstadoStock actual = stock.get(e.productoId());
    if (actual == null || !e.ocurridoEn().isBefore(actual.actualizadoEn())) {
      stock.put(e.productoId(), new ResumenAnalitico.EstadoStock(e.productoId(), e.nombreProducto(),
          e.stockActual(), e.umbral(), e.stockBajo(), e.ocurridoEn()));
    }
    if (e.stockBajo()) {
      alertas.addFirst(new ResumenAnalitico.AlertaStock(e.eventId(), e.productoId(), e.nombreProducto(),
          e.stockActual(), e.umbral(), e.pedidoId(), e.ocurridoEn()));
      while (alertas.size() > MAX_ALERTAS) {
        alertas.removeLast();
      }
    }
    return true;
  }

  public synchronized ResumenAnalitico resumen(int top) {
    BigDecimal ticket = totalPedidos == 0
        ? BigDecimal.ZERO
        : ingresos.divide(BigDecimal.valueOf(totalPedidos), 2, RoundingMode.HALF_UP);

    List<ResumenAnalitico.ProductoVendido> masVendidos = porProducto.entrySet().stream()
        .map(en -> new ResumenAnalitico.ProductoVendido(en.getKey(), en.getValue().nombre, en.getValue().unidades, en.getValue().ingresos))
        .sorted(Comparator.comparingLong(ResumenAnalitico.ProductoVendido::unidades).reversed()
            .thenComparing(ResumenAnalitico.ProductoVendido::nombre))
        .limit(Math.max(1, top))
        .toList();

    List<ResumenAnalitico.EstadoStock> bajos = stock.values().stream()
        .filter(ResumenAnalitico.EstadoStock::stockBajo)
        .sorted(Comparator.comparingInt(ResumenAnalitico.EstadoStock::stockActual)
            .thenComparing(ResumenAnalitico.EstadoStock::nombre))
        .toList();

    return new ResumenAnalitico(Instant.now(), totalPedidos, ingresos, ticket, unidades, clientes.size(),
        masVendidos, bajos, List.copyOf(alertas),
        new ResumenAnalitico.Flujo(eventosPedidos, eventosInventario, duplicados, ultimoEvento));
  }

  /** Marca el evento como visto; devuelve false si ya lo estaba. Recuerda solo los últimos MAX_EVENTOS_RECORDADOS. */
  private boolean recordar(String clave) {
    if (!vistos.add(clave)) {
      duplicados++;
      return false;
    }
    if (vistos.size() > MAX_EVENTOS_RECORDADOS) {
      Iterator<String> it = vistos.iterator();
      it.next();
      it.remove();
    }
    return true;
  }

  private void actualizarUltimo(Instant momento) {
    if (momento != null && (ultimoEvento == null || momento.isAfter(ultimoEvento))) {
      ultimoEvento = momento;
    }
  }

  private static final class Acumulado {
    private String nombre = "";
    private long unidades;
    private BigDecimal ingresos = BigDecimal.ZERO;
  }
}
