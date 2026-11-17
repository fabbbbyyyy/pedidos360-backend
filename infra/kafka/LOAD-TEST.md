# Prueba de carga de Kafka, Outbox y DLT

Esta guía ejecuta una carga controlada mediante
[scripts/load-test.js](./scripts/load-test.js).

Por defecto genera:

- 100 eventos en total.
- 75 eventos validos.
- 15 eventos invalidos que deben llegar a `orders.events.dlt`.
- 10 eventos sin `data` para probar reintentos del publisher.

La prueba escribe datos sinteticos en la tabla Outbox y en las metricas de
Analytics. Ejecutarla solo cuando se acepte contaminar esas metricas con datos
de prueba.

## 1. Desplegar los workers actualizados

Ejecutar el workflow `Deploy Kafka workers` antes de la prueba. La imagen debe
incluir el consumidor de analytics con soporte para `orders.events.dlt`.

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

## 3. Abrir los monitores

Abrir cuatro terminales SSH en la carpeta `pedidos360-kafka`.

### Terminal 1: eventos normales

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic orders.events \
  --bootstrap-server localhost:29092 \
  --group "debug-events-$(date +%s)" \
  --consumer-property auto.offset.reset=latest \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

### Terminal 2: eventos DLT

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic orders.events.dlt \
  --bootstrap-server localhost:29092 \
  --group "debug-dlt-$(date +%s)" \
  --consumer-property auto.offset.reset=latest \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

### Terminal 3: publicador Outbox

```bash
docker compose logs -f --tail=50 outbox-publisher
```

### Terminal 4: consumidor Analytics

```bash
docker compose logs -f --tail=50 analytics
```

Los workers muestran principalmente conexiones, particiones y errores. Los
mensajes completos se observan en las terminales de Kafka.

## 4. Ejecutar la carga

Abrir una quinta terminal y ejecutar:

```bash
LOAD_TEST_TOTAL=100 \
LOAD_TEST_DLT_COUNT=15 \
LOAD_TEST_RETRY_COUNT=10 \
docker compose exec -T outbox-publisher \
node - < scripts/load-test.js
```

El comando imprimira un resultado similar a:

```json
{
  "mode": "insert",
  "runId": "load-test-1763330000000",
  "total": 100,
  "valid": 75,
  "dlt": 15,
  "retry": 10
}
```

Guardar el `runId`. Se necesitara para limpiar los registros despues.

## 5. Flujo esperado

Eventos validos:

```text
Outbox -> outbox-publisher -> orders.events -> analytics -> DynamoDB Analytics
```

Eventos invalidos para DLT:

```text
Outbox -> outbox-publisher -> orders.events -> analytics -> orders.events.dlt
```

Eventos para reintentos:

```text
Outbox -> outbox-publisher -> error -> PENDING
```

Los eventos enviados correctamente deben quedar en Outbox con:

```text
status = PUBLISHED
gsi1pk = STATUS#PUBLISHED
```

## 6. Verificar el consumer lag

Ejecutar:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:29092 \
  --group analytics-service \
  --describe
```

Esperar a que `LAG` vuelva a `0`. Los 10 eventos sin `data` no deben bloquear
el consumidor de analytics porque el publisher los deja en `PENDING`.

## 7. Limpiar la ejecucion

La limpieza debe ser el ultimo paso, despues de revisar los eventos y logs.
Usar el mismo `runId` y el mismo total:

```bash
LOAD_TEST_MODE=cleanup \
LOAD_TEST_RUN_ID=load-test-1763330000000 \
LOAD_TEST_TOTAL=100 \
docker compose exec -T outbox-publisher \
node - < scripts/load-test.js
```

La limpieza elimina unicamente los registros de Outbox generados por ese
`runId`. No elimina mensajes que ya fueron enviados a Kafka, mensajes de la
DLT ni metricas ya procesadas en Analytics.

No borrar manualmente las tablas DynamoDB para limpiar una prueba.
