package ec.udla.bikestore.producer;

import ec.udla.bikestore.messaging.MessageBus;
import ec.udla.bikestore.messaging.Topology;
import ec.udla.bikestore.model.Order;
import ec.udla.bikestore.model.OrderItem;
import ec.udla.bikestore.model.OrderRequest;
import ec.udla.bikestore.util.Json;
import ec.udla.bikestore.util.Log;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Convierte la petición "Ordenar" en un mensaje JSON y lo publica en el bróker.
 * No espera al cobro ni al e-mail: responde de inmediato (asincronía).
 */
public class OrderProducer {

    private static final String NAME = "OrderProducer";
    private final MessageBus bus;

    public OrderProducer(MessageBus bus) {
        this.bus = bus;
    }

    public Order ordenar(OrderRequest req) throws Exception {
        String pedidoId = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        double total = req.items().stream().mapToDouble(OrderItem::subtotal).sum();

        Order order = new Order(pedidoId, req.cliente(), req.email(), req.items(),
                Math.round(total * 100.0) / 100.0, Order.PENDING, OffsetDateTime.now().toString());

        bus.publish(Topology.RK_ORDER_CREATED, Json.write(order));
        Log.event(NAME, pedidoId, "Pedido recibido (%d items, $%.2f) -> publicado order.created",
                req.items().size(), order.total());
        return order;
    }
}
