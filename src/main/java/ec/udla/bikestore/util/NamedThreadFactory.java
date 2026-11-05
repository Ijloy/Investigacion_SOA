package ec.udla.bikestore.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/** Pone nombres legibles a los hilos (payment-1, email-1, http-api-1...) para que el log sea claro. */
public class NamedThreadFactory implements ThreadFactory {
    private final String prefijo;
    private final AtomicInteger contador = new AtomicInteger(1);

    public NamedThreadFactory(String prefijo) {
        this.prefijo = prefijo;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, prefijo + "-" + contador.getAndIncrement());
        t.setDaemon(true);
        return t;
    }
}
