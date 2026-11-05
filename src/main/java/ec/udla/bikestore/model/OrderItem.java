package ec.udla.bikestore.model;

/** Línea del pedido: un producto, cuántas unidades y su precio unitario. */
public record OrderItem(String producto, int cantidad, double precioUnitario) {
    public double subtotal() {
        return cantidad * precioUnitario;
    }
}
