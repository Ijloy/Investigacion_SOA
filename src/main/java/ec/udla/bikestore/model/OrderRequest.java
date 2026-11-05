package ec.udla.bikestore.model;

import java.util.List;

/** Cuerpo JSON que el front-end envía a POST /ordenar. */
public record OrderRequest(String cliente, String email, List<OrderItem> items) {

    /** Devuelve null si es válido, o el motivo del error. */
    public String validar() {
        if (cliente == null || cliente.isBlank()) return "cliente es obligatorio";
        if (email == null || !email.contains("@")) return "email inválido";
        if (items == null || items.isEmpty()) return "el pedido no tiene items";
        for (OrderItem i : items) {
            if (i.cantidad() <= 0 || i.precioUnitario() < 0) return "item inválido: " + i.producto();
        }
        return null;
    }
}
