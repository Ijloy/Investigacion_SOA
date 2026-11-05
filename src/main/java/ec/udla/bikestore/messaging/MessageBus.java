package ec.udla.bikestore.messaging;

import java.util.List;
import java.util.Map;

/**
 * Abstracción del bróker. Los workers solo conocen esta interfaz, no RabbitMQ:
 * si mañana se cambia a otro bróker, solo se escribe otra implementación.
 *
 * Implementaciones:
 *  - RabbitMqBus  -> RabbitMQ real (lo que se entrega).
 *  - InMemoryBus  -> simulación en memoria para correr la demo sin Docker.
 */
public interface MessageBus extends AutoCloseable {

    /** Qué hacer con un mensaje después de procesarlo. */
    enum Outcome {
        ACK,         // procesado OK: se elimina de la cola
        RETRY,       // falló: reintentar más tarde (cola .retry con TTL)
        DEAD_LETTER  // falló definitivamente: va a la Dead-Letter Queue
    }

    @FunctionalInterface
    interface Handler {
        Outcome handle(Message message) throws Exception;
    }

    /** Declara exchanges, colas y bindings (idempotente). */
    void setup() throws Exception;

    /** Publica un evento en el exchange de eventos con la routing key dada. */
    void publish(String routingKey, String jsonBody) throws Exception;

    /** Registra un consumidor con N hilos sobre una cola. */
    void subscribe(String queue, String workerName, int concurrency, Handler handler) throws Exception;

    /** Lee (sin consumir) hasta max mensajes de una cola. Útil para inspeccionar la DLQ. */
    List<String> peek(String queue, int max) throws Exception;

    @Override
    void close();

    /** Utilidad común: describe un mensaje muerto para el inspector de la DLQ. */
    static String describe(String body, Map<String, Object> headers) {
        return "headers=" + headers + "\n    body=" + body;
    }
}
