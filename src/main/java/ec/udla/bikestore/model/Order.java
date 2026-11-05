package ec.udla.bikestore.model;

import java.util.List;

/**
 * Mensaje que viaja por el bróker (se serializa a JSON).
 * paymentStatus: PENDING -> PAID | FAILED
 */
public record Order(
        String pedidoId,
        String cliente,
        String email,
        List<OrderItem> items,
        double total,
        String paymentStatus,
        String createdAt) {

    public static final String PENDING = "PENDING";
    public static final String PAID = "PAID";
    public static final String FAILED = "FAILED";

    /** Los records son inmutables: cambiar el estado crea una copia. */
    public Order withPaymentStatus(String nuevoEstado) {
        return new Order(pedidoId, cliente, email, items, total, nuevoEstado, createdAt);
    }
}
