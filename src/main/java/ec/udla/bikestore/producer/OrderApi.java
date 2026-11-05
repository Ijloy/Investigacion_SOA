package ec.udla.bikestore.producer;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import ec.udla.bikestore.model.Order;
import ec.udla.bikestore.model.OrderRequest;
import ec.udla.bikestore.util.Json;
import ec.udla.bikestore.util.Log;
import ec.udla.bikestore.util.NamedThreadFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * API HTTP mínima (servidor incluido en el JDK, sin Spring):
 *   POST /ordenar  -> publica el pedido y responde 202 Accepted con el pedidoId
 *   GET  /         -> página de prueba con el botón "Confirmar pedido"
 */
public class OrderApi {

    private final HttpServer server;

    public OrderApi(OrderProducer producer, int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(4, new NamedThreadFactory("http-api")));

        server.createContext("/ordenar", ex -> {
            try {
                addCors(ex);
                if ("OPTIONS".equals(ex.getRequestMethod())) {
                    send(ex, 204, "");
                    return;
                }
                if (!"POST".equals(ex.getRequestMethod())) {
                    send(ex, 405, Json.write(Map.of("error", "Use POST")));
                    return;
                }
                OrderRequest req = Json.read(new String(ex.getRequestBody().readAllBytes(),
                        StandardCharsets.UTF_8), OrderRequest.class);
                String error = req.validar();
                if (error != null) {
                    send(ex, 400, Json.write(Map.of("error", error)));
                    return;
                }
                Order order = producer.ordenar(req);
                send(ex, 202, Json.write(Map.of(
                        "pedidoId", order.pedidoId(),
                        "total", order.total(),
                        "estado", "EN_PROCESO",
                        "mensaje", "Pedido recibido. El cobro y el e-mail se procesan en segundo plano.")));
            } catch (IllegalArgumentException e) {
                send(ex, 400, Json.write(Map.of("error", e.getMessage())));
            } catch (Exception e) {
                Log.info("OrderApi", "Error publicando pedido: %s", e.getMessage());
                send(ex, 503, Json.write(Map.of("error", "Bróker no disponible, intente de nuevo")));
            }
        });

        server.createContext("/", ex -> {
            try (InputStream in = OrderApi.class.getResourceAsStream("/public/index.html")) {
                byte[] html = in == null ? "<h1>BikeStore</h1>".getBytes(StandardCharsets.UTF_8) : in.readAllBytes();
                ex.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
                ex.sendResponseHeaders(200, html.length);
                try (OutputStream os = ex.getResponseBody()) {
                    os.write(html);
                }
            }
        });
    }

    public void start() {
        server.start();
        Log.info("OrderApi", "API escuchando en http://localhost:%d  (POST /ordenar)",
                server.getAddress().getPort());
    }

    public void stop() {
        server.stop(0);
    }

    /** Permite llamar al API desde un front-end en otro puerto (p. ej. Vite en :5173). */
    private static void addCors(HttpExchange ex) {
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        ex.getResponseHeaders().set("Access-Control-Allow-Methods", "POST, OPTIONS");
        ex.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
    }

    private static void send(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) {
            try (OutputStream os = ex.getResponseBody()) {
                os.write(bytes);
            }
        }
        ex.close();
    }
}
