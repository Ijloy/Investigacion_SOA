package ec.udla.bikestore.messaging;

/**
 * Nombres de exchanges, colas y routing keys. Un solo lugar = un solo "contrato" de mensajería.
 *
 *  OrderProducer --order.created--> [bikestore.events (topic)] --> payment.queue --> PaymentWorker
 *                                                                       |  fallo (reintento)
 *                                                                       v
 *                                                       payment.queue.retry (TTL 2 s) --vuelve--> payment.queue
 *                                                                       |  fallo tras 3 reintentos (nack)
 *                                                                       v
 *                                                       [bikestore.dlx (direct)] --> payment.dlq
 *
 *  PaymentWorker --payment.paid / payment.failed--> [bikestore.events] --> email.queue --> EmailWorker
 */
public final class Topology {
    private Topology() {}

    // Exchanges
    public static final String EXCHANGE_EVENTS = "bikestore.events"; // topic
    public static final String EXCHANGE_DLX = "bikestore.dlx";       // direct

    // Routing keys (eventos de negocio)
    public static final String RK_ORDER_CREATED = "order.created";
    public static final String RK_PAYMENT_PAID = "payment.paid";
    public static final String RK_PAYMENT_FAILED = "payment.failed";
    public static final String RK_PAYMENT_ANY = "payment.*";
    public static final String RK_PAYMENT_DLQ = "payment.dlq";

    // Colas
    public static final String Q_PAYMENT = "payment.queue";
    public static final String Q_PAYMENT_RETRY = Q_PAYMENT + ".retry";
    public static final String Q_PAYMENT_DLQ = "payment.dlq";
    public static final String Q_EMAIL = "email.queue";

    // Política de reintentos
    public static final int MAX_RETRIES = 3;          // 1 intento + 3 reintentos = 4 intentos
    public static final int RETRY_DELAY_MS = 2_000;   // espera entre reintentos
    public static final String HEADER_RETRY = "x-retry-count";

    /** Convención: la cola de reintento de X se llama X.retry */
    public static String retryQueueOf(String queue) {
        return queue + ".retry";
    }
}
