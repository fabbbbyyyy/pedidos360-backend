# Kafka local en KRaft

Este entorno levanta un broker Kafka local en modo KRaft, sin ZooKeeper, y
DynamoDB Local para probar el consumidor de analítica. Es un entorno de
desarrollo de un solo nodo y no representa todavía la configuración segura de
producción.

## Requisitos

- Docker Desktop con Compose.
- Puerto `9092` disponible.

## Iniciar Kafka y DynamoDB Local

Desde PowerShell:

```powershell
Copy-Item .env.example .env
docker compose --profile local up -d
docker compose ps
```

Esperar a que `pedidos360-kafka` aparezca como `healthy`, crear los topics y
preparar la tabla de analítica:

```powershell
.\create-topics.ps1
$env:AWS_REGION = "us-east-1"
$env:DYNAMODB_ENDPOINT = "http://localhost:8000"
$env:ANALYTICS_TABLE = "pedidos360-analytics-local"
npm run analytics:setup-local
```

Topics creados:

- `orders.events`: 3 particiones, retención de 7 días.
- `orders.events.dlt`: 1 partición, retención de 30 días.

## Workers Docker

La imagen de los workers se define en `Dockerfile.workers`. Para construirla:

```powershell
docker compose --profile workers build
```

Los workers se mantienen detrás del perfil `workers` porque requieren sus tablas
y configuración runtime. El consumidor de analítica puede iniciarse después de
crear `pedidos360-analytics-local`:

```powershell
docker compose --profile local --profile workers up -d analytics
```

El publicador de outbox requiere además que exista `OUTBOX_TABLE` y que sus
variables Kafka/SASL/TLS estén configuradas para el entorno correspondiente.
Si el worker se detiene después de reclamar un evento, recupera automáticamente
los leases `PROCESSING` cuyo `leaseUntil` ya expiró y los devuelve a `PENDING`.

## Carga de prueba y DLT

`scripts/load-test.js` inserta eventos directamente en la tabla Outbox usando
el instance profile de la EC2. Por defecto genera 5.000 eventos válidos para
medir el flujo normal. Ejecutarlo únicamente en un entorno de prueba. Para
mantener la medición de throughput separada, los casos de error se ejecutan
después en una prueba negativa de 25 eventos: 15 deben llegar a
`orders.events.dlt` y 10 sin `data` deben quedarse en `PENDING` mientras el
publisher reintenta.

```bash
docker compose exec -T \
  -e LOAD_TEST_TOTAL=5000 \
  -e LOAD_TEST_DLT_COUNT=0 \
  -e LOAD_TEST_RETRY_COUNT=0 \
  outbox-publisher node - < scripts/load-test.js
```

Guardar el `runId` que imprime. Después de que esta carga termine, ejecutar la
prueba negativa y guardar también su `runId`:

```bash
docker compose exec -T \
  -e LOAD_TEST_TOTAL=25 \
  -e LOAD_TEST_DLT_COUNT=15 \
  -e LOAD_TEST_RETRY_COUNT=10 \
  outbox-publisher node - < scripts/load-test.js
```

Las filas de Outbox de cada ejecución pueden limpiarse por separado usando su
`runId` y el mismo total (`5000` o `25`). La limpieza no elimina mensajes Kafka
ni revierte métricas ya escritas en Analytics. No reutilizar un `runId` en una
nueva ejecución. Los comandos de limpieza y verificación están en
[LOAD-TEST.md](./LOAD-TEST.md).

La imagen de workers debe incluir primero el consumidor con soporte para
`orders.events.dlt`. Verificar los resultados con:

```bash
docker compose logs -f --tail=100 outbox-publisher analytics
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic orders.events.dlt \
  --bootstrap-server localhost:29092 \
  --from-beginning \
  --property print.key=true \
  --property print.offset=true
```

El script no usa credenciales estáticas: el SDK de AWS toma el instance
profile de la EC2.

## Configuración de producción

Usar `.env.production.example` como referencia, sin copiar credenciales reales
al repositorio. En producción `KAFKA_TLS=true` es obligatorio para los workers:
deben existir la CA en `KAFKA_CA_PATH` y las credenciales SASL/SCRAM. El
directorio `infra/kafka/secrets/` está excluido de Git.

El workflow manual `.github/workflows/deploy-kafka-workers.yml` crea o actualiza
la EC2 mediante CloudFormation, publica la imagen en GHCR y configura la
instancia mediante SSM. Requiere configurar en GitHub:

- `KAFKA_EC2_STACK_NAME`
- `KAFKA_ALLOWED_KAFKA_CIDR` (opcional; por defecto usa la red privada creada)
- `KAFKA_INSTANCE_PROFILE_NAME` (opcional; se busca automáticamente asociado a `LabRole`)
- `KAFKA_IAM_ROLE_NAME` (opcional, por defecto `LabRole`)
- `KAFKA_INSTANCE_TYPE` (opcional, por defecto `t3.small`)
- `KAFKA_EC2_APP_DIR`
- `GHCR_USERNAME`
- `GHCR_READ_TOKEN`
- `KAFKA_ENV_B64`
- `KAFKA_CA_B64` (solo cuando `KAFKA_TLS=true`)

En `KAFKA_ENV_B64`, usa `__KAFKA_ELASTIC_IP__` como marcador en
`KAFKA_ADVERTISED_HOST`; el workflow lo reemplaza por la Elastic IP creada por
CloudFormation. Los workers usan automáticamente `kafka:29092` dentro de la
red Docker; los clientes externos usan la Elastic IP en el puerto `9092`.

El workflow crea automáticamente una VPC `10.42.0.0/16`, una subnet pública
`10.42.1.0/24`, Internet Gateway, tabla de rutas, EC2, Elastic IP, Security
Group y disco EBS cifrado. La instancia usa un instance profile existente con
permisos de SSM y DynamoDB, normalmente `LabRole` en AWS Academy.

`KAFKA_ALLOWED_KAFKA_CIDR` puede omitirse para permitir Kafka solo desde la VPC.
Si necesitas conectarte desde tu computador, configúralo como tu IP pública
seguida de `/32`; no uses `0.0.0.0/0`. Para producción, deja
`DYNAMODB_ENDPOINT` vacío para que el SDK use DynamoDB de AWS.

## Probar particiones y mensajes

Listar detalles:

```powershell
docker compose exec kafka `
  /opt/kafka/bin/kafka-topics.sh `
  --describe `
  --topic orders.events `
  --bootstrap-server localhost:9092
```

Consumir desde el inicio:

```powershell
docker compose exec kafka `
  /opt/kafka/bin/kafka-console-consumer.sh `
  --topic orders.events `
  --from-beginning `
  --bootstrap-server localhost:9092
```

Publicar un evento de prueba con `orderId` como key:

```powershell
'local-1|{"eventId":"ORDER_CREATED#local-1","eventType":"ORDER_CREATED","source":"local","occurredAt":"2026-11-16T18:00:00.000Z","data":{"orderId":"local-1"}}' |
  docker compose exec -T kafka `
  /opt/kafka/bin/kafka-console-producer.sh `
  --topic orders.events `
  --bootstrap-server localhost:9092 `
  --property "parse.key=true" `
  --property "key.separator=|"
```

## Detener y limpiar

Detener sin borrar datos:

```powershell
docker compose down
```

Eliminar también el volumen local:

```powershell
docker compose down -v
```

No ejecutar `down -v` si se necesitan conservar los eventos locales.
