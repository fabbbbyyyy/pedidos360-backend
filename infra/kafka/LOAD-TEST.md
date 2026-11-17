# Prueba de carga de Kafka, Outbox y DLT

Esta guía ejecuta una carga controlada mediante
[scripts/load-test.js](./scripts/load-test.js).

Por defecto genera 5.000 eventos validos. La prueba negativa se ejecuta por
separado con 25 eventos: 15 invalidos que deben llegar a
`orders.events.dlt` y 10 sin `data` para probar reintentos del publisher.
Separarlas evita que los reintentos permanentes distorsionen la medicion de
throughput.

Las pruebas escriben datos sinteticos en Outbox, Kafka y Analytics. Ejecutarlas
solo en un entorno de prueba y aceptando que las metricas de Analytics y los
mensajes Kafka permaneceran despues de limpiar Outbox.

## 1. Desplegar los workers actualizados

Ejecutar el workflow `Deploy Kafka workers` antes de la prueba. La imagen debe
incluir el publisher que envía cada lote en una sola solicitud a Kafka y el
consumidor de analytics que reintenta conflictos transitorios de DynamoDB y
soporta `orders.events.dlt`.

## 2. Copiar el script a la EC2

Desde la carpeta del proyecto Kafka:

```bash
cd /ruta/pedidos360-kafka
mkdir -p scripts
```

Copiar el contenido de `scripts/load-test.js` del repositorio a:

```text
/ruta/pedidos360-kafka/scripts/load-test.js
```

## 3. Abrir una sesion en cada EC2

Usar AWS Systems Manager; no hace falta abrir SSH ni conectarse a Kafka desde
Internet. En la consola AWS:

1. Abrir **EC2 > Instances** y filtrar por el stack `pedidos360-kafka`.
2. Seleccionar la instancia del broker 1 y elegir **Connect > Session Manager > Connect**.
3. Repetir en los brokers 2 y 3. Abrir cada sesion en una pestana/terminal
   separada.
4. En cada sesion, confirmar el nodo y el directorio:

```bash
cd /opt/pedidos360-kafka
grep '^KAFKA_NODE_ID=' .env
docker compose ps
```

La topologia esperada es:

| Sesion | `KAFKA_NODE_ID` | Servicios esperados |
|---|---:|---|
| Broker 1 | 1 | `kafka`, `analytics`, `outbox-publisher` |
| Broker 2 | 2 | `kafka`, `analytics`, `outbox-publisher` |
| Broker 3 | 3 | `kafka`, `analytics` |

Si `KAFKA_EC2_APP_DIR` en GitHub Actions no es `/opt/pedidos360-kafka`, usar
ese directorio en los comandos siguientes.

### Que dejar abierto durante la carga

Abrir estas terminales antes de insertar eventos:

**Broker 1, sesion de monitor: seguir errores del publisher:**

```bash
cd /opt/pedidos360-kafka
docker compose logs -f --since=2m outbox-publisher
```

**Broker 2, sesion de monitor: seguir el segundo publisher:**

```bash
cd /opt/pedidos360-kafka
docker compose logs -f --since=2m outbox-publisher
```

Los dos publishers reclaman filas de Outbox mediante leases; no se espera que
cada uno procese exactamente la mitad. Los logs muestran errores y conexiones,
no una linea por cada publicacion exitosa. Un mismo `eventId` puede aparecer
en ambos logs si un fallo lo devuelve a `PENDING` y el otro publisher lo
reclama en un intento posterior; el lease evita que ambos lo reclamen a la vez.

**Broker 3, sesion de monitor: seguir Analytics:**

```bash
cd /opt/pedidos360-kafka
docker compose logs -f --since=2m analytics
```

**Broker 1, otra sesion: monitorear offsets y lag cada 10 segundos:**

```bash
cd /opt/pedidos360-kafka
while true; do
  printf '\n--- %s ---\n' "$(date -u +%T)"
  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:29092 \
    --group analytics-service \
    --describe
  sleep 10
done
```

En cada particion, `LOG-END-OFFSET` avanza cuando Kafka recibe mensajes y
`LAG` indica cuantos mensajes le faltan al grupo Analytics por procesar.
**`LAG=0` por si solo no significa que terminaron los 5.000**: Analytics puede
estar al dia mientras los publishers siguen produciendo.

Para contar en vivo los mensajes de esta ejecucion, abrir otra sesion (por
ejemplo, broker 3), reemplazar `load-test-PEGAR_RUN_ID` por el `runId` que
imprimio el script y ejecutar:

```bash
LOAD_RUN_ID='load-test-PEGAR_RUN_ID'
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic orders.events \
  --from-beginning \
  --timeout-ms 30000 \
  | grep --line-buffered -F "$LOAD_RUN_ID" \
  | awk '{ count++; if (count % 100 == 0) { printf "Eventos de esta carga: %d\n", count; fflush() } }
         END { printf "Total publicado para este runId: %d\n", count }'
```

Este consumer de observacion usa un grupo temporal independiente y no altera
los offsets de `analytics-service`. Cuando termina por 30 segundos sin nuevos
mensajes, el total debe ser `5000`. Si sigue imprimiendo cada 100, la carga
sigue avanzando. Presionar `Ctrl+C` para detenerlo manualmente.

Para ver el payload de **cada evento nuevo** (carga o pedido normal), abrir una
sola terminal adicional en cualquiera de los brokers antes de iniciar la
carga o crear el pedido:

```bash
cd /opt/pedidos360-kafka
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic orders.events \
  --property print.timestamp=true \
  --property print.partition=true \
  --property print.offset=true \
  --property print.key=true
```

El consumidor muestra timestamp, particion, offset, key y el payload JSON de
cada mensaje publicado desde que se inicia. No agregar `--from-beginning` para
evitar volcar el historial, ni usar el grupo `analytics-service`: esta pantalla
observa mensajes publicados en Kafka, no confirma que Analytics ya termino de
procesarlos. Presionar `Ctrl+C` para detenerla.

## 4. Ejecutar la carga de 5.000 eventos validos

En una sesion nueva de broker 1, ejecutar:

```bash
docker compose exec -T \
  -e LOAD_TEST_TOTAL=5000 \
  -e LOAD_TEST_DLT_COUNT=0 \
  -e LOAD_TEST_RETRY_COUNT=0 \
  outbox-publisher \
node - < scripts/load-test.js
```

El comando imprimira un resultado similar a:

```json
{
  "mode": "insert",
  "runId": "load-test-1763330000000",
  "total": 5000,
  "valid": 5000,
  "dlt": 0,
  "retry": 0
}
```

Guardar el `runId` y asignarlo en la terminal, por ejemplo
`LOAD_RUN_ID='load-test-<id>'`. Esperar a que el publicador termine y el
consumer group vuelva a `LAG=0` antes de limpiar Outbox o iniciar la prueba
negativa. El publisher procesa lotes de hasta `OUTBOX_BATCH_SIZE` (25 por
defecto) y no espera el intervalo de polling mientras la consulta siga
devolviendo un lote completo; esto acelera el drenaje, pero el tiempo final
debe medirse en el entorno desplegado.

## 5. Ejecutar la prueba negativa

Abrir la terminal del consumidor DLT antes de insertar los eventos y ejecutar:

```bash
docker compose exec -T \
  -e LOAD_TEST_TOTAL=25 \
  -e LOAD_TEST_DLT_COUNT=15 \
  -e LOAD_TEST_RETRY_COUNT=10 \
  outbox-publisher \
node - < scripts/load-test.js
```

Guardar el `runId` y asignarlo en una terminal, por ejemplo
`FAILURE_RUN_ID='load-test-PEGAR_RUN_ID'`. El resultado esperado es `valid=0`,
`dlt=15` y `retry=10`.

En una sesion de broker 3, observar los mensajes DLT de esta ejecucion:

```bash
FAILURE_RUN_ID='load-test-PEGAR_RUN_ID'
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic orders.events.dlt \
  --from-beginning \
  --timeout-ms 30000 \
  | grep --line-buffered -F "$FAILURE_RUN_ID" \
  | awk '{ count++; if (count % 5 == 0) { printf "Mensajes DLT de esta prueba: %d\n", count; fflush() } }
         END { printf "Total DLT para este runId: %d\n", count }'
```

El total esperado es 15. Analytics rechaza los mensajes sin `eventType` y
publica el original en `orders.events.dlt`. En broker 1 y broker 2, seguir en
vivo los errores de los diez eventos sin `data`:

```bash
docker compose logs -f --since=2m outbox-publisher \
  | grep --line-buffered -F "$FAILURE_RUN_ID"
```

Esos eventos fallan en el publisher, permanecen `PENDING` y acumulan
`attempts`; no llegan a Kafka ni a la DLT. Limpiar esas filas despues de
confirmar los reintentos para que el publisher no siga procesandolas.
Los 15 eventos DLT conservan `data` y solo omiten `eventType`: deben llegar a
Kafka y luego a `orders.events.dlt`. Si tambien aparecen con error
`data.orderId`, detener nuevas pruebas y revisar el publisher/despliegue:
un evento sin `data` no debe impedir publicar los demas del lote.

## 6. Flujo esperado

```text
Validos: Outbox -> outbox-publisher -> orders.events -> analytics -> DynamoDB Analytics
DLT: Outbox -> outbox-publisher -> orders.events -> analytics -> orders.events.dlt
Retry: Outbox -> error en outbox-publisher -> permanece PENDING
```

Los eventos enviados correctamente deben quedar en Outbox con:

```text
status = PUBLISHED
gsi1pk = STATUS#PUBLISHED
```

## 7. Verificar el consumer lag y la DLT

Ejecutar:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:29092 \
  --group analytics-service \
  --describe
```

Esperar a que `LAG` vuelva a `0` despues de la carga valida y despues de la
prueba negativa. Confirmar los 15 mensajes de prueba en `orders.events.dlt`.
Los 10 eventos sin `data` se quedan en Outbox como `PENDING` con `attempts`
incrementandose; no bloquean el consumer group, pero el publisher seguira
reintentandolos hasta limpiar sus filas.

## 8. Limpiar las filas de Outbox

La limpieza debe ser el ultimo paso, despues de revisar los eventos, logs y
resultados. Ejecutarla una vez por cada `runId`, con el total correspondiente:

```bash
docker compose exec -T \
  -e LOAD_TEST_MODE=cleanup \
  -e LOAD_TEST_RUN_ID="$LOAD_RUN_ID" \
  -e LOAD_TEST_TOTAL=5000 \
  outbox-publisher \
node - < scripts/load-test.js

docker compose exec -T \
  -e LOAD_TEST_MODE=cleanup \
  -e LOAD_TEST_RUN_ID="$FAILURE_RUN_ID" \
  -e LOAD_TEST_TOTAL=25 \
  outbox-publisher \
node - < scripts/load-test.js
```

La limpieza elimina unicamente los registros de Outbox generados por ese
`runId`. No elimina mensajes enviados a Kafka ni marcadores y metricas de
Analytics. Cada ejecucion debe usar un `runId` nuevo; no reutilizar uno
anterior.

No borrar manualmente las tablas DynamoDB para limpiar una prueba.
