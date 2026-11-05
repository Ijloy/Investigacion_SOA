package ec.udla.bikestore.messaging;

import java.util.HashMap;
import java.util.Map;

/** Mensaje recibido: cuerpo JSON + headers (aquí viaja el contador de reintentos). */
public record Message(String body, Map<String, Object> headers) {

    public Message {
        headers = headers == null ? new HashMap<>() : new HashMap<>(headers);
    }

    /** 0 en la primera entrega, 1 en el primer reintento, etc. */
    public int retryCount() {
        Object v = headers.get(Topology.HEADER_RETRY);
        return v instanceof Number n ? n.intValue() : 0;
    }

    public Message withRetryCount(int n) {
        Map<String, Object> h = new HashMap<>(headers);
        h.put(Topology.HEADER_RETRY, n);
        return new Message(body, h);
    }
}
