package ec.udla.bikestore;

import ec.udla.bikestore.messaging.InMemoryBus;
import ec.udla.bikestore.messaging.MessageBus;
import ec.udla.bikestore.messaging.RabbitMqBus;
import ec.udla.bikestore.messaging.Topology;
import ec.udla.bikestore.model.OrderItem;
import ec.udla.bikestore.model.OrderRequest;
import ec.udla.bikestore.producer.OrderApi;
import ec.udla.bikestore.producer.OrderProducer;
import ec.udla.bikestore.util.Log;
import ec.udla.bikestore.workers.EmailWorker;
import ec.udla.bikestore.workers.PaymentWorker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Punto de entrada. Cada componente puede correr en su propio proceso (como servicios reales)
 * o todos juntos para la demo.
 *
 * Uso:  java -jar target/bikestore-async.jar <modo> [opciones]
 *   modos:   all (por defecto) | api | payment | email | dlq | demo
 *   opciones: --memory          usa el bus en memoria (sin RabbitMQ)
 *             --orders=N        (demo) cantidad de pedidos a enviar, por defecto 6
 *             --fail-rate=0.5   probabilidad de fallo del cobro
 *             --port=8080       puerto del API
 *
 * Variables de entorno: RABBIT_HOST, RABBIT_PORT, RABBIT_USER, RABBIT_PASS
 */
public class App {

    public static void main(String[] args) throws Exception {
        String mode = "all";
        Map<String, String> opts = new HashMap<>();
        for (String a : args) {
            if (a.startsWith("--")) {
                String[] kv = a.substring(2).split("=", 2);
                opts.put(kv[0], kv.length > 1 ? kv[1] : "true");
            } else {
                mode = a;
            }
        }
        double failRate = Double.parseDouble(opts.getOrDefault("fail-rate", "0.5"));
        int port = Integer.parseInt(opts.getOrDefault("port", "8080"));

        MessageBus bus = opts.containsKey("memory")
                ? new InMemoryBus()
                : new RabbitMqBus(env("RABBIT_HOST", "localhost"), Integer.parseInt(env("RABBIT_PORT", "5672")),
                                  env("RABBIT_USER", "guest"), env("RABBIT_PASS", "guest"));
        bus.setup();

        switch (mode) {
            case "api" -> startApi(bus, port);
            case "payment" -> startPayment(bus, failRate);
            case "email" -> startEmail(bus);
            case "dlq" -> {
                printDlq(bus);
                bus.close();
                return;
            }
            case "demo" -> {
                startPayment(bus, failRate);
                startEmail(bus);
                runDemo(bus, Integer.parseInt(opts.getOrDefault("orders", "6")));
                bus.close();
                return;
            }
            default -> { // all
                startPayment(bus, failRate);
                startEmail(bus);
                startApi(bus, port);
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(bus::close));
        Log.info("App", "Modo '%s' en ejecución. Ctrl+C para salir.", mode);
        new CountDownLatch(1).await(); // mantener vivo el proceso
    }

    private static void startApi(MessageBus bus, int port) throws Exception {
        new OrderApi(new OrderProducer(bus), port).start();
    }

    private static void startPayment(MessageBus bus, double failRate) throws Exception {
        bus.subscribe(Topology.Q_PAYMENT, "payment", 2, new PaymentWorker(bus, failRate));
    }

    private static void startEmail(MessageBus bus) throws Exception {
        bus.subscribe(Topology.Q_EMAIL, "email", 1, new EmailWorker());
    }

    /** Envía N pedidos de ejemplo, espera a que terminen los reintentos e imprime la DLQ. */
    private static void runDemo(MessageBus bus, int n) throws Exception {
        OrderProducer producer = new OrderProducer(bus);
        for (int i = 1; i <= n; i++) {
            producer.ordenar(new OrderRequest("Cliente " + i, "cliente" + i + "@mail.com", List.of(
                    new OrderItem("Bicicleta MTB", 1, 650 + i * 10),
                    new OrderItem("Casco", 1, 45.50))));
        }
        long espera = (long) (Topology.MAX_RETRIES + 1) * (Topology.RETRY_DELAY_MS + 700) + 2_000;
        Log.info("App", "Demo: %d pedidos enviados, esperando %d ms a que terminen los reintentos...", n, espera);
        Thread.sleep(espera);
        printDlq(bus);
    }

    private static void printDlq(MessageBus bus) throws Exception {
        List<String> dead = bus.peek(Topology.Q_PAYMENT_DLQ, 50);
        Log.info("DLQ", "%d mensaje(s) en %s", dead.size(), Topology.Q_PAYMENT_DLQ);
        dead.forEach(d -> System.out.println("  - " + d));
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
