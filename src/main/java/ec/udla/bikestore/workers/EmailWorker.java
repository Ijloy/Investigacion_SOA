package ec.udla.bikestore.workers;

import ec.udla.bikestore.messaging.Message;
import ec.udla.bikestore.messaging.MessageBus;
import ec.udla.bikestore.messaging.MessageBus.Outcome;
import ec.udla.bikestore.model.Order;
import ec.udla.bikestore.util.Json;
import ec.udla.bikestore.util.Log;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumidor 2: envía el e-mail de confirmación SOLO si paymentStatus = PAID.
 * Es idempotente: el bróker garantiza entrega "al menos una vez", así que un mismo
 * pedido podría llegar dos veces; se recuerda a quién ya se escribió.
 */
public class EmailWorker implements MessageBus.Handler {

    private static final String NAME = "EmailWorker";
    private final Set<String> yaEnviados = ConcurrentHashMap.newKeySet();

    @Override
    public Outcome handle(Message message) throws Exception {
        Order order = Json.read(message.body(), Order.class);
        String id = order.pedidoId();

        if (!Order.PAID.equals(order.paymentStatus())) {
            Log.event(NAME, id, "paymentStatus=%s -> NO se envía e-mail de confirmación",
                    order.paymentStatus());
            return Outcome.ACK;
        }
        if (!yaEnviados.add(id)) {
            Log.event(NAME, id, "Duplicado detectado -> e-mail ya enviado, se ignora");
            return Outcome.ACK;
        }

        Thread.sleep(150); // simula el servidor SMTP
        Log.event(NAME, id, "[OK] E-mail de confirmación enviado a %s (total $%.2f)",
                order.email(), order.total());
        return Outcome.ACK;
    }
}
