package ec.udla.bikestore.workers;

import ec.udla.bikestore.messaging.Message;
import ec.udla.bikestore.messaging.MessageBus;
import ec.udla.bikestore.messaging.MessageBus.Outcome;
import ec.udla.bikestore.model.Order;
import ec.udla.bikestore.util.Json;
import ec.udla.bikestore.util.Log;

import java.util.concurrent.ThreadLocalRandom;

import static ec.udla.bikestore.messaging.Topology.*;

/**
 * Consumidor 1: simula el cobro con tarjeta.
 * - Falla con probabilidad failRate (0.5 por defecto) para ejercitar los reintentos.
 * - Hasta 3 reintentos; si sigue fallando, el pedido va a la Dead-Letter Queue.
 * - Publica el resultado (payment.paid / payment.failed) para que otros servicios reaccionen.
 */
public class PaymentWorker implements MessageBus.Handler {

    private static final String NAME = "PaymentWorker";
    private final MessageBus bus;
    private final double failRate;

    public PaymentWorker(MessageBus bus, double failRate) {
        this.bus = bus;
        this.failRate = failRate;
    }

    @Override
    public Outcome handle(Message message) throws Exception {
        Order order = Json.read(message.body(), Order.class);
        String id = order.pedidoId();
        int intento = message.retryCount() + 1;
        int maxIntentos = MAX_RETRIES + 1;

        Log.event(NAME, id, "Cobrando $%.2f a tarjeta de %s (intento %d/%d)",
                order.total(), order.cliente(), intento, maxIntentos);

        Thread.sleep(ThreadLocalRandom.current().nextInt(200, 600)); // latencia de la pasarela

        boolean aprobado = ThreadLocalRandom.current().nextDouble() >= failRate;
        if (aprobado) {
            bus.publish(RK_PAYMENT_PAID, Json.write(order.withPaymentStatus(Order.PAID)));
            Log.event(NAME, id, "[OK] Pago APROBADO -> evento payment.paid publicado");
            return Outcome.ACK;
        }

        if (message.retryCount() < MAX_RETRIES) {
            Log.event(NAME, id, "[FALLO] Pasarela rechazó el cobro. Reintento %d/%d en %d ms",
                    message.retryCount() + 1, MAX_RETRIES, RETRY_DELAY_MS);
            return Outcome.RETRY;
        }

        bus.publish(RK_PAYMENT_FAILED, Json.write(order.withPaymentStatus(Order.FAILED)));
        Log.event(NAME, id, "[DLQ] Falló tras %d reintentos -> enviado a %s para análisis offline",
                MAX_RETRIES, Q_PAYMENT_DLQ);
        return Outcome.DEAD_LETTER;
    }
}
