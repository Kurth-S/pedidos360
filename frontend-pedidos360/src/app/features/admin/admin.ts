import { Component, computed, inject, signal } from '@angular/core';
import { CurrencyPipe, DatePipe } from '@angular/common';
import { ApiService } from '../../core/api.service';
import { Pedido } from '../../core/models';

/** Ruta protegida por rol Admin (guard en el frontend + hasRole('ADMIN') en el BFF). */
@Component({
  selector: 'app-admin',
  imports: [CurrencyPipe, DatePipe],
  templateUrl: './admin.html',
  styleUrl: './admin.scss',
})
export class Admin {
  private readonly api = inject(ApiService);
  protected readonly pedidos = signal<Pedido[] | null>(null);
  protected readonly error = signal<string | null>(null);
  protected readonly ventas = computed(() => (this.pedidos() ?? []).reduce((acc, p) => acc + p.total, 0));

  constructor() {
    this.api.pedidosAdmin().subscribe({
      next: (lista) => this.pedidos.set(lista),
      error: (err) => this.error.set(ApiService.mensajeDeError(err)),
    });
  }
}
