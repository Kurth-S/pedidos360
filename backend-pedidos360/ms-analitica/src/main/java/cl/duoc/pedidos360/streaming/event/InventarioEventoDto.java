package cl.duoc.pedidos360.streaming.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import java.time.Instant;

/**
 * Evento del tópico {@code pedidos360.inventario.eventos} (lo publica ms-productos).
 * Es el ESTADO del stock de un producto tras un pedido; la clave del mensaje es el id del producto,
 * por eso el tópico usa cleanup.policy=compact,delete (importa el último valor por producto).
 */
public record InventarioEventoDto(
    @NotBlank String eventId,
    @NotBlank String tipo,
    @NotNull Instant ocurridoEn,
    @NotNull Long productoId,
    @NotBlank String nombreProducto,
    @NotNull @PositiveOrZero Integer stockActual,
    @NotNull @PositiveOrZero Integer umbral,
    boolean stockBajo,
    Long pedidoId,
    @PositiveOrZero Integer cantidadPedida) {

  public static final String STOCK_REVISADO = "STOCK_REVISADO";
}
