package cl.duoc.pedidos360.analitica;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** Indicadores calculados en tiempo real a partir de los eventos de Kafka. */
public record ResumenAnalitico(
    Instant generadoEn,
    long totalPedidos,
    BigDecimal ingresosTotales,
    BigDecimal ticketPromedio,
    long unidadesVendidas,
    int clientesUnicos,
    List<ProductoVendido> topProductos,
    List<EstadoStock> productosConStockBajo,
    List<AlertaStock> ultimasAlertas,
    Flujo flujo) {

  public record ProductoVendido(Long productoId, String nombre, long unidades, BigDecimal ingresos) {}

  public record EstadoStock(Long productoId, String nombre, int stockActual, int umbral, boolean stockBajo, Instant actualizadoEn) {}

  public record AlertaStock(String eventId, Long productoId, String nombre, int stockActual, int umbral, Long pedidoId, Instant ocurridoEn) {}

  /** Estado del flujo de datos: cuántos eventos se procesaron y cuántos duplicados se descartaron. */
  public record Flujo(long eventosPedidos, long eventosInventario, long duplicadosDescartados, Instant ultimoEvento) {}
}
