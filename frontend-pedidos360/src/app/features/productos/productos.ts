import { Component, computed, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { CurrencyPipe } from '@angular/common';
import { ApiService } from '../../core/api.service';
import { AuthService } from '../../core/auth.service';
import { CarritoService } from '../../core/carrito.service';
import { Producto, ProductoRequest } from '../../core/models';

const FORM_VACIO: ProductoRequest = { nombre: '', descripcion: '', categoria: 'General', precio: 0, stock: 0 };

@Component({
  selector: 'app-productos',
  imports: [FormsModule, CurrencyPipe],
  templateUrl: './productos.html',
  styleUrl: './productos.scss',
})
export class Productos {
  protected readonly api = inject(ApiService);
  protected readonly auth = inject(AuthService);
  protected readonly carrito = inject(CarritoService);

  protected readonly productos = signal<Producto[]>([]);
  protected readonly cargando = signal(true);
  protected readonly error = signal<string | null>(null);
  protected readonly mensaje = signal<string | null>(null);
  protected readonly form = signal<ProductoRequest>({ ...FORM_VACIO });
  protected readonly guardando = signal(false);
  protected readonly busqueda = signal('');
  protected readonly filtrados = computed(() => {
    const q = this.busqueda().trim().toLowerCase();
    if (!q) return this.productos();
    return this.productos().filter((p) => `${p.nombre} ${p.categoria}`.toLowerCase().includes(q));
  });

  constructor() {
    this.cargar();
  }

  cargar(): void {
    this.cargando.set(true);
    this.error.set(null);
    this.api.listarProductos().subscribe({
      next: (lista) => {
        this.productos.set(lista);
        this.cargando.set(false);
      },
      error: (err) => {
        this.error.set(ApiService.mensajeDeError(err));
        this.cargando.set(false);
      },
    });
  }

  /** Matiz (0-359) derivado de la categoría, para colorear cada tarjeta. */
  tono(categoria: string): number {
    let h = 0;
    for (const c of categoria ?? '') h = (h * 31 + c.charCodeAt(0)) % 360;
    return h;
  }

  agregar(p: Producto): void {
    this.carrito.agregar(p);
    this.mensaje.set(`"${p.nombre}" agregado al pedido.`);
  }

  /** Solo Admin: POST /api/productos (el backend responde 403 si el token no trae el rol). */
  guardar(): void {
    const datos = this.form();
    if (!datos.nombre.trim()) {
      this.error.set('El nombre es obligatorio.');
      return;
    }
    this.guardando.set(true);
    this.error.set(null);
    this.api.crearProducto(datos).subscribe({
      next: (p) => {
        this.mensaje.set(`Producto "${p.nombre}" creado.`);
        this.guardando.set(false);
        this.form.set({ ...FORM_VACIO });
        this.cargar();
      },
      error: (err) => {
        this.error.set(ApiService.mensajeDeError(err));
        this.guardando.set(false);
      },
    });
  }

  actualizarCampo<K extends keyof ProductoRequest>(campo: K, valor: ProductoRequest[K]): void {
    this.form.update((f) => ({ ...f, [campo]: valor }));
  }
}
