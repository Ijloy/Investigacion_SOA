package ec.udla.bikestore.messaging;

import ec.udla.bikestore.util.Log;
import ec.udla.bikestore.util.NamedThreadFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static ec.udla.bikestore.messaging.Topology.*;

/**
 * Simulación en memoria del mismo comportamiento que RabbitMqBus (exchange topic,
 * reintentos con retardo y DLQ). Sirve para probar la lógica sin instalar nada.
 * NO es durable: si el proceso termina, los mensajes se pierden.
 */
public class InMemoryBus implements MessageBus {

    private record Binding(Pattern pattern, String queue) {}

    private final Map<String, BlockingQueue<Message>> queues = new ConcurrentHashMap<>();
    private final Map<String, String> deadLetterTarget = new ConcurrentHashMap<>();
    private final List<Binding> bindings = new CopyOnWriteArrayList<>();
    private final List<ExecutorService> pools = new ArrayList<>();
    private final ScheduledExecutorService retryScheduler =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("retry-timer"));

    @Override
    public void setup() {
        declare(Q_PAYMENT);
        declare(Q_PAYMENT_DLQ);
        declare(Q_EMAIL);
        bind(RK_ORDER_CREATED, Q_PAYMENT);
        bind(RK_PAYMENT_ANY, Q_EMAIL);
        deadLetterTarget.put(Q_PAYMENT, Q_PAYMENT_DLQ);
        Log.info("InMemoryBus", "Topología en memoria lista (modo demo, sin RabbitMQ)");
    }

    private void declare(String q) {
        queues.putIfAbsent(q, new LinkedBlockingQueue<>());
    }

    /** Traduce el patrón topic de AMQP (* = una palabra, # = cero o más) a regex. */
    private void bind(String topicPattern, String queue) {
        String regex = topicPattern.replace(".", "\\.").replace("*", "[^.]+").replace("#", ".*");
        bindings.add(new Binding(Pattern.compile(regex), queue));
    }

    @Override
    public void publish(String routingKey, String jsonBody) {
        for (Binding b : bindings) {
            if (b.pattern().matcher(routingKey).matches()) {
                queues.get(b.queue()).add(new Message(jsonBody, new HashMap<>()));
            }
        }
    }

    @Override
    public void subscribe(String queue, String workerName, int concurrency, Handler handler) {
        BlockingQueue<Message> q = queues.get(queue);
        ExecutorService pool = Executors.newFixedThreadPool(concurrency, new NamedThreadFactory(workerName));
        pools.add(pool);
        for (int i = 0; i < concurrency; i++) {
            pool.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    Message msg;
                    try {
                        msg = q.take();
                    } catch (InterruptedException e) {
                        return;
                    }
                    Outcome outcome;
                    try {
                        outcome = handler.handle(msg);
                    } catch (Exception e) {
                        Log.info(workerName, "Error inesperado (%s): mensaje a DLQ", e.getMessage());
                        outcome = Outcome.DEAD_LETTER;
                    }
                    switch (outcome) {
                        case ACK -> { }
                        case RETRY -> {
                            Message next = msg.withRetryCount(msg.retryCount() + 1);
                            retryScheduler.schedule(() -> q.add(next), RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
                        }
                        case DEAD_LETTER -> {
                            String dlq = deadLetterTarget.get(queue);
                            if (dlq != null) {
                                Map<String, Object> h = new HashMap<>(msg.headers());
                                h.put("x-first-death-queue", queue);
                                h.put("x-first-death-reason", "rejected");
                                queues.get(dlq).add(new Message(msg.body(), h));
                            }
                        }
                    }
                }
            });
        }
        Log.info(workerName, "Escuchando cola '%s' con %d hilo(s)", queue, concurrency);
    }

    @Override
    public List<String> peek(String queue, int max) {
        List<String> out = new ArrayList<>();
        for (Message m : queues.get(queue)) {
            if (out.size() >= max) break;
            out.add(MessageBus.describe(m.body(), m.headers()));
        }
        return out;
    }

    @Override
    public void close() {
        pools.forEach(ExecutorService::shutdownNow);
        retryScheduler.shutdownNow();
    }
}
