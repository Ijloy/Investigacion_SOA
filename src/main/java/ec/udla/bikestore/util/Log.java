package ec.udla.bikestore.util;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

/**
 * Log de eventos en consola. Cada línea lleva: timestamp, componente, pedidoId y thread,
 * tal como pide el enunciado.
 */
public final class Log {
    private Log() {}

    public static void event(String componente, String pedidoId, String formato, Object... args) {
        String linea = String.format(Locale.ROOT, "%s | %-13s | pedidoId=%-8s | thread=%-16s | %s",
                OffsetDateTime.now().truncatedTo(ChronoUnit.MILLIS),
                componente,
                pedidoId == null ? "-" : pedidoId,
                Thread.currentThread().getName(),
                String.format(Locale.ROOT, formato, args));
        System.out.println(linea);
    }

    public static void info(String componente, String formato, Object... args) {
        event(componente, null, formato, args);
    }
}
