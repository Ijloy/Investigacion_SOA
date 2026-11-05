package ec.udla.bikestore.messaging;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import ec.udla.bikestore.util.Log;
import ec.udla.bikestore.util.NamedThreadFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static ec.udla.bikestore.messaging.Topology.*;

/**
 * Implementación con RabbitMQ.
 *
 * Mecanismos usados:
 *  - Mensajes persistentes (deliveryMode=2) en colas durables.
 *  - Publisher confirms: el productor espera a que el bróker confirme que guardó el mensaje.
 *  - ACK manual + prefetch=1: un mensaje no se borra hasta que el worker lo procesa.
 *  - Reintento con retardo: cola payment.queue.retry con TTL que, al expirar, devuelve el mensaje.
 *  - Dead-Letter Exchange: basicNack(requeue=false) envía el mensaje a payment.dlq.
 */
public class RabbitMqBus implements MessageBus {

    private final ConnectionFactory factory = new ConnectionFactory();
    private final Connection publishConnection;
    private final Channel publishChannel;
    private final List<Connection> consumerConnections = new ArrayList<>();
    private final List<ExecutorService> executors = new ArrayList<>();

    public RabbitMqBus(String host, int port, String user, String password) throws Exception {
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(user);
        factory.setPassword(password);
        factory.setAutomaticRecoveryEnabled(true); // reconecta solo si el bróker se reinicia

        publishConnection = factory.newConnection("bikestore-publisher");
        publishChannel = publishConnection.createChannel();
        publishChannel.confirmSelect(); // activa publisher confirms
        Log.info("RabbitMqBus", "Conectado a RabbitMQ en %s:%d", host, port);
    }

    @Override
    public void setup() throws Exception {
        Channel ch = publishChannel;
        ch.exchangeDeclare(EXCHANGE_EVENTS, "topic", true);
        ch.exchangeDeclare(EXCHANGE_DLX, "direct", true);

        // Cola principal de pagos: si un mensaje se rechaza sin requeue, va al DLX
        Map<String, Object> paymentArgs = new HashMap<>();
        paymentArgs.put("x-dead-letter-exchange", EXCHANGE_DLX);
        paymentArgs.put("x-dead-letter-routing-key", RK_PAYMENT_DLQ);
        ch.queueDeclare(Q_PAYMENT, true, false, false, paymentArgs);
        ch.queueBind(Q_PAYMENT, EXCHANGE_EVENTS, RK_ORDER_CREATED);

        // Cola de reintento: nadie la consume; el mensaje "espera" RETRY_DELAY_MS y,
        // al expirar, el bróker lo devuelve a payment.queue a través del exchange por defecto ("")
        Map<String, Object> retryArgs = new HashMap<>();
        retryArgs.put("x-message-ttl", RETRY_DELAY_MS);
        retryArgs.put("x-dead-letter-exchange", "");
        retryArgs.put("x-dead-letter-routing-key", Q_PAYMENT);
        ch.queueDeclare(Q_PAYMENT_RETRY, true, false, false, retryArgs);

        // Dead-Letter Queue
        ch.queueDeclare(Q_PAYMENT_DLQ, true, false, false, null);
        ch.queueBind(Q_PAYMENT_DLQ, EXCHANGE_DLX, RK_PAYMENT_DLQ);

        // Cola de e-mails: escucha payment.paid y payment.failed
        ch.queueDeclare(Q_EMAIL, true, false, false, null);
        ch.queueBind(Q_EMAIL, EXCHANGE_EVENTS, RK_PAYMENT_ANY);

        Log.info("RabbitMqBus", "Topología declarada (exchanges, colas, retry y DLQ)");
    }

    @Override
    public void publish(String routingKey, String jsonBody) throws Exception {
        publishRaw(EXCHANGE_EVENTS, routingKey, jsonBody, new HashMap<>());
    }

    /** Un Channel no es thread-safe: se sincroniza porque varios hilos publican. */
    private synchronized void publishRaw(String exchange, String routingKey, String body,
                                         Map<String, Object> headers) throws Exception {
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .deliveryMode(2) // persistente
                .headers(headers)
                .build();
        publishChannel.basicPublish(exchange, routingKey, props, body.getBytes(StandardCharsets.UTF_8));
        publishChannel.waitForConfirmsOrDie(5_000);
    }

    @Override
    public void subscribe(String queue, String workerName, int concurrency, Handler handler) throws Exception {
        // Una conexión por worker con su propio pool de hilos con nombre (payment-1, payment-2...)
        ExecutorService pool = Executors.newFixedThreadPool(concurrency, new NamedThreadFactory(workerName));
        Connection conn = factory.newConnection(pool, "bikestore-" + workerName);
        executors.add(pool);
        consumerConnections.add(conn);

        for (int i = 0; i < concurrency; i++) {
            Channel ch = conn.createChannel();
            ch.basicQos(1); // máximo 1 mensaje sin confirmar por consumidor
            ch.basicConsume(queue, false, (tag, delivery) -> {
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                Map<String, Object> headers = delivery.getProperties().getHeaders();
                Message msg = new Message(new String(delivery.getBody(), StandardCharsets.UTF_8), headers);

                Outcome outcome;
                try {
                    outcome = handler.handle(msg);
                } catch (Exception e) {
                    Log.info(workerName, "Error inesperado (%s): mensaje a DLQ", e.getMessage());
                    outcome = Outcome.DEAD_LETTER;
                }

                try {
                    switch (outcome) {
                        case ACK -> ch.basicAck(deliveryTag, false);
                        case RETRY -> {
                            // 1) copia a la cola de reintento con el contador +1, 2) ack del original
                            Message next = msg.withRetryCount(msg.retryCount() + 1);
                            publishRaw("", retryQueueOf(queue), next.body(), next.headers());
                            ch.basicAck(deliveryTag, false);
                        }
                        case DEAD_LETTER -> ch.basicNack(deliveryTag, false, false); // -> DLX -> DLQ
                    }
                } catch (Exception e) {
                    throw new IOException("No se pudo confirmar el mensaje", e);
                }
            }, tag -> Log.info(workerName, "Consumidor cancelado"));
        }
        Log.info(workerName, "Escuchando cola '%s' con %d hilo(s)", queue, concurrency);
    }

    @Override
    public List<String> peek(String queue, int max) throws Exception {
        List<String> out = new ArrayList<>();
        try (Channel ch = publishConnection.createChannel()) {
            long lastTag = -1;
            for (int i = 0; i < max; i++) {
                GetResponse r = ch.basicGet(queue, false);
                if (r == null) break;
                lastTag = r.getEnvelope().getDeliveryTag();
                out.add(MessageBus.describe(new String(r.getBody(), StandardCharsets.UTF_8),
                        r.getProps().getHeaders()));
            }
            if (lastTag >= 0) ch.basicNack(lastTag, true, true); // devolverlos: solo estamos mirando
        }
        return out;
    }

    @Override
    public void close() {
        for (Connection c : consumerConnections) closeQuietly(c);
        closeQuietly(publishConnection);
        executors.forEach(ExecutorService::shutdownNow);
    }

    private static void closeQuietly(Connection c) {
        try {
            c.close();
        } catch (Exception ignored) {
            // cierre best-effort
        }
    }
}
