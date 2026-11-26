package cl.duoc.pedidos360.analitica;

import static org.assertj.core.api.Assertions.assertThat;

import cl.duoc.pedidos360.streaming.event.InventarioEventoDto;
import cl.duoc.pedidos360.streaming.event.PedidoEventoDto;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class AnaliticaStoreTest {

  private static final Instant T0 = Instant.parse("2026-10-20T09:40:00Z");

  private static PedidoEventoDto pedido(String id, long pedidoId, String cliente, long productoId, String nombre, int cantidad, String subtotal) {
    return new PedidoEventoDto(id, PedidoEventoDto.PEDIDO_CREADO, T0.plusSeconds(pedidoId), pedidoId, cliente,
        new BigDecimal(subtotal), List.of(new PedidoEventoDto.Item(productoId, nombre, cantidad, new BigDecimal(subtotal))));
  }

  private static InventarioEventoDto stock(String id, long segundos, long productoId, String nombre, int stock, boolean bajo) {
    return new InventarioEventoDto(id, InventarioEventoDto.STOCK_REVISADO, T0.plusSeconds(segundos), productoId, nombre, stock, 5, bajo, 1L, 1);
  }

  @Test
  void acumulaVentasIngresosYTopProductos() {
    AnaliticaStore store = new AnaliticaStore();
    store.registrarPedido(pedido("e1", 1, "A", 1, "Laptop", 1, "599990"));
    store.registrarPedido(pedido("e2", 2, "A", 2, "Cable USB", 2, "11980"));
    store.registrarPedido(pedido("e3", 3, "B", 2, "Cable USB", 1, "5990"));

    ResumenAnalitico r = store.resumen(5);

    assertThat(r.totalPedidos()).isEqualTo(3);
    assertThat(r.ingresosTotales()).isEqualByComparingTo("617960");
    assertThat(r.ticketPromedio()).isEqualByComparingTo("205986.67");
    assertThat(r.unidadesVendidas()).isEqualTo(4);
    assertThat(r.clientesUnicos()).isEqualTo(2);
    assertThat(r.topProductos().get(0).nombre()).isEqualTo("Cable USB");
    assertThat(r.topProductos().get(0).unidades()).isEqualTo(3);
  }

  @Test
  void descartaEventosDuplicadosPorEventId() {
    AnaliticaStore store = new AnaliticaStore();
    PedidoEventoDto p = pedido("e1", 1, "A", 1, "Laptop", 1, "599990");

    assertThat(store.registrarPedido(p)).isTrue();
    assertThat(store.registrarPedido(p)).isFalse();

    ResumenAnalitico r = store.resumen(5);
    assertThat(r.totalPedidos()).isEqualTo(1);
    assertThat(r.flujo().duplicadosDescartados()).isEqualTo(1);
  }

  @Test
  void elUltimoStockConocidoPrevaleceYGeneraAlertas() {
    AnaliticaStore store = new AnaliticaStore();
    store.registrarInventario(stock("s1", 61, 2, "Cable USB", 3, true));
    store.registrarInventario(stock("s2", 121, 2, "Cable USB", 2, true));
    store.registrarInventario(stock("s0", 10, 2, "Cable USB", 99, false)); // evento antiguo: no pisa el estado reciente

    ResumenAnalitico r = store.resumen(5);

    assertThat(r.productosConStockBajo()).hasSize(1);
    assertThat(r.productosConStockBajo().get(0).stockActual()).isEqualTo(2);
    assertThat(r.ultimasAlertas()).hasSize(2);
    assertThat(r.ultimasAlertas().get(0).eventId()).isEqualTo("s2");
  }
}
