# BikeStore Async

Proyecto de la materia **Diseño y Arquitectura de Software** (UDLA), parte de una investigación sobre SOA, microservicios y mensajería asíncrona.

## ¿Qué problema resuelve?

Cuando alguien compra una bicicleta, la tienda tiene que cobrar y enviar un correo. Si la página espera a que todo termine, el cliente se queda esperando, y si el cobro falla el pedido puede perderse.

Aquí la tienda recibe el pedido, responde de inmediato y deja el trabajo en una cola. Otros programas lo procesan por detrás, cada uno a su ritmo.

## ¿Qué hicimos?

Construimos tres componentes en Java 17 que se comunican mediante **RabbitMQ**, un bróker de mensajería:

| Componente | Qué hace |
|---|---|
| **OrderProducer** | Recibe el pedido en `POST /ordenar`, lo publica en la cola y responde `202` |
| **PaymentWorker** | Simula el cobro, con un 50 % de probabilidad de fallo |
| **EmailWorker** | Envía el correo de confirmación solo si el pago fue aprobado |

```
Pedido ─► OrderProducer ─► payment.queue ─► PaymentWorker ─┬─ aprobado ─► email.queue ─► EmailWorker ─► correo
                               ▲                           │
                               │                           ├─ falla ─► payment.queue.retry (espera 2 s y vuelve)
                               └───────────────────────────┘
                                                           └─ falla 4 veces ─► payment.dlq (sin correo)
```

Si un cobro falla, se reintenta hasta 3 veces, con 2 segundos de espera entre intentos. Si sigue fallando, el pedido pasa a la **DLQ** (*dead-letter queue*), una cola donde se guarda para revisarlo después.

## ¿Por qué RabbitMQ?

Trae de fábrica los reintentos con retardo, la DLQ y el enrutamiento a varios consumidores. También es open source y fácil de levantar. Kafka obligaría a programar la DLQ a mano, y su principal ventaja (guardar y reproducir el historial de mensajes) no se necesita aquí.

## ¿Qué probamos?

Estas pruebas se hicieron con RabbitMQ 3.13.7 instalado localmente:

| Prueba | Resultado |
|---|---|
| 6 pedidos con 50 % de fallo | 6 cobrados y 6 correos enviados (el fallo es aleatorio y puede variar) |
| 1 pedido con 100 % de fallo | 4 intentos, termina en la DLQ y no se envía correo |
| 6 pedidos con 100 % de fallo | 24 intentos, 6 en la DLQ y ningún correo |
| EmailWorker apagado | Los mensajes esperan en la cola y los correos salen al volver a encenderlo |
| Reinicio completo de RabbitMQ | No se pierde ningún mensaje pendiente |
| PaymentWorker se cae a mitad de un cobro | El pedido vuelve a la cola, se reprocesa y sale un solo correo |

## Cómo ejecutarlo

Necesitas **JDK 17 o superior**.

**1. Compilar**
```powershell
.\mvnw.cmd clean package
```

**2. Probarlo sin instalar nada más**

Usa una cola simulada en memoria, sin RabbitMQ:
```powershell
java -jar target/bikestore-async.jar demo --memory
```

**3. Ejecutarlo con RabbitMQ**

Con [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado:
```powershell
docker compose up -d
java -jar target/bikestore-async.jar all
```
Luego abre:
- La tienda: http://localhost:8080
- La consola de RabbitMQ: http://localhost:15672 (usuario y clave `guest`)

> El archivo `docker-compose.yml` usa la misma serie de RabbitMQ (3.13), pero este paso con Docker aún no se ha probado. Las pruebas de arriba se hicieron con una instalación local.

### Otros comandos

| Comando | Qué hace |
|---|---|
| `java -jar target/bikestore-async.jar demo` | Envía 6 pedidos de ejemplo |
| `java -jar target/bikestore-async.jar demo --orders=1 --fail-rate=1` | Fuerza un pedido a la DLQ |
| `java -jar target/bikestore-async.jar dlq` | Muestra la DLQ sin vaciarla |
| `java -jar target/bikestore-async.jar api`, `payment` o `email` | Ejecuta cada componente por separado, en su propia terminal |

## Limitaciones

- El cobro y el correo son simulados: no se usa una pasarela de pago ni un servidor de correo reales.
- EmailWorker recuerda en memoria los correos ya enviados. Si se reinicia, podría repetir uno cuando le llega un mensaje duplicado.
- El proyecto no tiene pruebas automáticas; las pruebas de arriba se hicieron ejecutándolo a mano.
