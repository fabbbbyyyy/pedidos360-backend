# Plan de implementación: Kafka KRaft en EC2

## 1. Objetivo y decisiones confirmadas

Integrar Kafka KRaft en tres EC2, ejecutado con Docker, sin retirar SNS/SQS:

- SNS/SQS y Kafka coexistirán permanentemente.
- Cada evento continuará llegando a SNS/SQS y se publicará a Kafka desde la outbox DynamoDB.
- El clúster tendrá exactamente tres EC2 `t3.medium`, en la misma AZ.
- Cada EC2 ejecutará un nodo KRaft combinado `broker,controller`.
- Se creará un clúster Kafka nuevo; no se migrarán los datos Kafka del clúster anterior.
- Se acepta la interrupción durante la transición al clúster nuevo.
- La publicación a Kafka seguirá usando el worker que consulta la outbox; no se usará un bridge SNS/SQS ni DynamoDB Streams como ruta objetivo.
- La entrega será **at-least-once**; los consumidores serán idempotentes.
- Se usarán topics de dominio, tres particiones iniciales y `orderId` como clave.
- Se usará `orders.events` y `orders.events.dlt`.
- Retención: 7 días para eventos normales y 30 días para DLT.
- RF=3 y `min.insync.replicas=2` para topics de negocio, DLT e internal topics; productores con `acks=all`.
- Para el PoC, PLAINTEXT solo dentro de la red privada; TLS/SASL queda diferido.
- Se conserva el puerto externo Kafka 9092 y el CIDR actual verificado; los brokers anunciarán direcciones privadas.
- Los workers usarán las tres direcciones privadas de los brokers como bootstrap.
- No se crearán EIP nuevas salvo que el preflight demuestre que son necesarias.
- La administración será exclusivamente mediante SSM o VPN.
- CloudWatch recibirá logs, métricas y alarmas.
- GitHub Actions ejecutará actualizaciones mediante SSM, aceptando una caída durante el despliegue.
- Se reutilizará exclusivamente el `LabRole` existente; no se presupone la creación de roles IAM nuevos.
- Las tres EC2 y sus datos persistentes no se eliminarán al terminar pruebas ni durante la limpieza de datos de prueba; quedarán generando costo hasta su eliminación manual.

## 2. Estado actual y punto de integración

El backend actual ya publica:

- `ORDER_CREATED` desde `createOrder`.
- `ORDER_STATUS_CHANGED` desde `changeOrderStatus`.

SNS distribuye los mensajes a tres colas SQS, cada una con su DLQ:

- Email.
- Shipping.
- Status.

Las colas existentes continúan atendiendo Email, Shipping y Status. Kafka recibe los eventos por el worker de outbox, no consumiendo esas colas ni mediante una cola SNS/SQS adicional.

El proyecto de referencia contiene topics, grupos de consumidores, reintentos y DLT, pero usa ZooKeeper y no implementa el consumo real de las colas del backend. Se reutilizarán sus ideas, no su configuración productiva.

## 3. Arquitectura objetivo

```text
 Lambda createOrder / changeOrderStatus
       |                         |
       +--> SNS --> SQS actuales |
       |                         +--> DynamoDB Outbox
       +----------------------------------+
                                          |
                              worker de polling de outbox
                                          |
                       +------------------+------------------+
                       |                  |                  |
                 EC2 broker 1       EC2 broker 2       EC2 broker 3
                 broker+controller  broker+controller  broker+controller
                       +------------------+------------------+
                                          |
                                  Kafka consumers
```

El clúster consta de tres EC2 `t3.medium` en la misma AZ. El worker de outbox publica a Kafka directamente; el bridge SNS/SQS no es parte de la arquitectura objetivo.

Los consumidores Kafka nuevos se incorporarán para `report` y `audit`. Los consumidores actuales de email, shipping y status permanecerán en SQS durante esta fase.

## 4. Contrato de eventos

Mantener un envelope común compatible con el payload actual de SNS:

```json
{
  "eventId": "uuid",
  "eventType": "ORDER_CREATED",
  "source": "pedidos360-backend",
  "occurredAt": "2026-11-16T18:00:00.000Z",
  "data": {}
}
```

Reglas:

- `eventId` identifica el evento y se usa para idempotencia.
- `eventType` conserva `ORDER_CREATED` y `ORDER_STATUS_CHANGED`.
- `orderId` debe existir en `data` y será la key Kafka.
- El JSON tendrá una versión explícita cuando el contrato evolucione.
- Validar productores y consumidores con los esquemas Zod existentes o un módulo común.
- No publicar mensajes inválidos; enviarlos a la DLT con causa y metadata.

## 5. Topics, particiones y offsets

### Topics iniciales

| Topic | Particiones | RF | `min.insync.replicas` | Retención | Uso |
|---|---:|---:|---:|---:|---|
| `orders.events` | 3 | 3 | 2 | 7 días | Eventos de pedidos |
| `orders.events.dlt` | 1 | 3 | 2 | 30 días | Mensajes que agotaron reintentos |

Aplicar RF=3 y `min.insync.replicas=2` también a los internal topics de Kafka, incluidos los que use el clúster para offsets y transacciones.

Kafka no permite reducir particiones. La cantidad debe parametrizarse desde el inicio.

### Claves y grupos

- Key: `orderId`.
- Esto conserva el orden de eventos del mismo pedido dentro de una partición.
- Cada servicio consumidor tendrá su propio `group.id`.
- Los offsets serán gestionados por Kafka con `enable.auto.commit=false`.
- El consumidor hará commit después de completar el procesamiento.
- Un reinicio retomará desde el último offset confirmado.
- El procesamiento debe ser idempotente porque puede existir redelivery.

Grupos previstos:

- `report-service`.
- `audit-service`.

## 6. Flujo de publicación y outbox

### Fase inicial del productor

1. La Lambda valida el comando.
2. Ejecuta la transacción de negocio en DynamoDB.
3. Registra el evento en una tabla outbox con `eventId`, tipo, payload, estado y timestamps.
4. Publica el evento a SNS para conservar el comportamiento actual.
5. Un proceso de entrega publica el evento a Kafka.
6. Marca el registro como publicado en Kafka.

La outbox evita depender de una operación no atómica entre DynamoDB, SNS y Kafka. El registro no debe marcarse como completado antes de recibir confirmación del destino.

### Idempotencia

- La clave primaria de la outbox debe impedir duplicar el mismo `eventId`.
- El worker de outbox conserva el polling actual por estado, con leases y recuperación de leases expirados.
- Los consumidores deben registrar eventos procesados o usar una operación idempotente en su almacenamiento.
- Kafka producer debe usar `acks=all`, `enable.idempotence=true` y reintentos limitados.

## 7. Worker de outbox -> Kafka

Mantener el worker Node.js de polling de la outbox como única ruta de publicación a Kafka. No implementar un consumer de DynamoDB Streams.

Responsabilidades:

1. Consultar periódicamente la outbox por su índice de estado y reclamar registros con leases.
2. Publicar en `orders.events` con `orderId` como key y `acks=all`.
3. Marcar `PUBLISHED` solo después del ack Kafka; ante fallo, volver a dejar el registro procesable.
4. Recuperar leases expirados y publicar el backlog cuando vuelva a estar disponible el worker/Kafka.
5. Configurar `KAFKA_BROKERS` con los tres endpoints privados, separados por comas.
6. No añadir una ruta de entrega mediante DynamoDB Streams.

## 8. Kafka KRaft en Docker

Crear artefactos versionados, sin ZooKeeper:

```text
infra/kafka/
  docker-compose.yml
  .env.example
  config/
  scripts/
    create-topics.sh
  README.md
```

El compose debe fijar versiones, nunca usar `latest`, y configurar:

- `process.roles=broker,controller` en cada uno de los tres nodos.
- `node.id` estable y distinto para cada nodo.
- `controller.quorum.voters` con los tres nodos y sus direcciones privadas.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`.
- Listener PLAINTEXT limitado a la red privada del PoC; conservar el puerto Kafka 9092 y el CIDR de acceso actual, verificados en preflight.
- Direcciones anunciadas privadas para los tres brokers. No anunciar Elastic IPs.
- RF=3 y `min.insync.replicas=2` para topics de negocio, DLT e internal topics.
- Volumen persistente cifrado por EC2, replicando el layout actual solo después de verificarlo y comprobar capacidad.
- Healthcheck del broker.
- Límites de memoria y disco.
- Reinicio automático del contenedor.

El PoC no habilita TLS/SASL; se difiere su incorporación. La resiliencia de broker se valida con tres nodos y RF=3/minISR=2, no mediante migración de datos del clúster anterior.

## 9. Seguridad del PoC y evolución de TLS/SASL

Para el PoC, usar PLAINTEXT únicamente en la red privada autorizada. Mantener el puerto 9092 y el CIDR vigente comprobado en preflight, sin ampliar el acceso ni anunciar endpoints públicos. TLS/SASL se difiere; no generar ni distribuir CA o credenciales como requisito de esta fase.

### Restricción de LabRole

El repositorio reutiliza `AWS_ROLE_ARN`/`LabRole` porque AWS Academy puede impedir crear roles. Antes de implementar, comprobar que el rol existente puede:

- Crear y administrar EC2, EBS, Security Groups y CloudWatch; EIP solo si el preflight determina que hace falta.
- Ejecutar comandos por SSM.
- Leer o escribir la configuración que se decida usar.
- Permitir a las Lambdas acceder a los recursos necesarios.

Si `LabRole` no tiene estos permisos, el despliegue debe detenerse con un diagnóstico. No se debe compensar desactivando TLS, exponiendo secretos en el repositorio o concediendo permisos amplios.

## 10. EC2 y red

### EC2

- Exactamente tres EC2 `t3.medium`, con los tres nodos en la misma AZ.
- Cada instancia ejecuta un broker y controller KRaft combinado.
- No migrar datos Kafka: inicializar un clúster nuevo; se acepta downtime durante la transición.
- Replicar el layout de almacenamiento persistente cifrado actual únicamente después de verificarlo en el stack/instancia vivos y comprobar capacidad suficiente para los tres nodos.
- No crear EIP nuevas salvo que el preflight lo requiera.
- Docker y Docker Compose instalados por User Data o script versionado.
- SSM Agent habilitado; sin SSH público.

### Security Group

Preservar el puerto 9092 y el CIDR exacto que se verifique en el Security Group actual. No adivinar ni ampliar el CIDR:

- Kafka 9092 únicamente desde el CIDR actual confirmado y compatible con el uso PLAINTEXT dentro de la red privada.
- Puerto de SSM según la configuración de la cuenta.
- Sin exponer Kafka UI ni endpoints administrativos a Internet.

Cada broker anunciará su dirección privada. El worker usará las tres direcciones privadas como bootstrap; no dependerá de una EIP.

## 11. Serverless y CloudFormation

Mantener `serverless.yml` y los recursos existentes para la outbox; no añadir recursos de bridge ni una fuente DynamoDB Streams. Configurar los workers Kafka con:

- Tabla DynamoDB outbox y permisos necesarios para el worker de polling.
- Topics, consumer groups y los tres endpoints privados de bootstrap.
- Puerto/CIDR actuales verificados para Kafka.
- Alarmas y log groups de CloudWatch.

Antes de actualizar el stack CloudFormation existente, registrar parámetros, outputs y recursos físicos vivos. El cambio puede reemplazar la EC2 física original; el resultado final debe tener exactamente tres EC2.

No mezclar secretos privados con valores versionados. El plan debe respetar el mecanismo actual de `.env` para configuración no sensible, pero la CA y credenciales no deben terminar en Git ni en el bundle de Lambda.

## 12. Despliegue con GitHub Actions y SSM

### Identificador KRaft y datos existentes

Configura `KAFKA_CLUSTER_ID` como variable del repositorio en GitHub (no es un secreto). Genéralo una sola vez con `kafka-storage.sh random-uuid` y conserva el mismo valor en todos los despliegues y brokers.

El workflow no migra datos Kafka ni cambia el factor de replicación de topics ya existentes. Úsalo con un stack de prueba y volúmenes Kafka nuevos; no reutilices un directorio de datos de otro clúster o `cluster.id`. Si `orders.events` o `orders.events.dlt` ya existen con un factor distinto de 3, el workflow se detendrá antes de desplegar los workers: migra esos topics explícitamente o prepara un clúster de prueba vacío.

### Distribución de workers para la prueba

Los brokers se ejecutan en las tres EC2. El consumer `analytics` se ejecuta en las tres, con el mismo `KAFKA_ANALYTICS_GROUP`, para que Kafka distribuya las tres particiones entre ellos. El `outbox-publisher` se ejecuta en broker 1 y broker 2; ambos compiten por eventos mediante los leases condicionales de DynamoDB. Así, la caída de una EC2 deja al menos un publisher activo.

Dos publishers duplican aproximadamente las consultas de polling de la outbox respecto de uno. La publicación sigue siendo al menos una vez, por lo que se debe conservar el procesamiento idempotente. Esta co-ubicación es para probar; para producción conviene desplegar los workers como servicios separados de los brokers y distribuirlos entre zonas de disponibilidad.

El bootstrap remoto espera a que termine `cloud-init` y verifica los paquetes de Docker antes de instalarlos. Si un comando SSM falla, revisa su salida y el estado de CloudFormation antes de relanzar: un fallo de SSM no implica que CloudFormation haya revertido el stack.

Fases del workflow:

1. Instalar dependencias y ejecutar tests.
2. Validar formato, configuración Kafka y archivos Compose.
3. Construir y fijar la imagen de los workers.
4. Publicar la imagen en el registro permitido por la cuenta.
5. Ejecutar SSM en la EC2 usando el rol ya configurado.
6. Descargar la versión fijada y recrear el compose.
7. Ejecutar healthchecks.
8. Crear/verificar topics.
9. Publicar resultado y logs.

El despliegue puede causar una caída, pero debe tener:

- Tag o digest de imagen reproducible.
- Archivo de configuración versionado.
- Comando de rollback a la versión anterior.
- Verificación posterior de los tres brokers, topics, worker de outbox y lag.

Preflight obligatorio antes del update: región y stack activos; parámetros, outputs, IDs y estado de recursos CloudFormation; VPC/subred/AZ; Security Group y CIDR vigente; EIP asociadas; y layout, cifrado, persistencia, tamaños y capacidad de discos. No iniciar el update hasta comparar el estado vivo con el destino acordado.

Se acepta el downtime de la transición y que CloudFormation sustituya la EC2 física original. El clúster nuevo no recibe migración de datos Kafka. Tras el update, comprobar exactamente tres EC2 activas. No eliminar EC2 ni volúmenes al finalizar pruebas o limpiar datos de prueba; definir explícitamente las políticas de retención para eliminación manual del stack y sustitución de recursos sin dejar instancias EC2 sobrantes.

## 13. Observabilidad y operación

Enviar a CloudWatch:

- Logs de broker.
- Logs del worker de outbox.
- Logs de consumidores.
- Errores de publicación.
- Mensajes en DLQ.
- Consumer lag.
- Uso de CPU, memoria y disco.
- Estado del volumen EBS.

Alarmas mínimas:

- Kafka no saludable.
- Disco por encima del umbral.
- Worker de outbox detenido.
- Mensajes en DLQ.
- Lag creciente.
- Fallos de publicación de la outbox.
- EC2 sin heartbeat de SSM.

Procedimientos documentados:

- Reiniciar worker de outbox.
- Reiniciar broker.
- Restaurar snapshot EBS.
- Reprocesar DLT.
- Rotar credenciales SASL.
- Rotar certificado/CA.
- Cambiar la Elastic IP si se pierde.
- Hacer rollback del despliegue.

## 14. Orden de implementación por fases

### Fase 0: permisos y preflight de la infraestructura viva

- Confirmar qué permite exactamente `LabRole`.
- Inspeccionar región, parámetros, outputs, recursos físicos y estado del stack CloudFormation existente.
- Registrar VPC, subred, AZ, Security Group y CIDR actual, EIPs, y layout/cifrado/tamaño/capacidad de almacenamiento.
- Confirmar capacidad/cuotas para tres EC2 `t3.medium` en la misma AZ.
- No iniciar si no se puede preservar el CIDR/puerto vigente o replicar el almacenamiento cifrado y persistente con capacidad suficiente.
- No avanzar si no se puede desplegar la infraestructura mínima con los permisos disponibles.

### Fase 1: contrato y módulos compartidos

- Extraer envelope y validadores comunes.
- Añadir tests para `eventId`, `eventType`, `occurredAt`, `orderId` y versionado.
- Definir configuración de topics y consumer groups.

### Fase 2: outbox y permisos

- Mantener la tabla outbox y el polling por estado/lease.
- Añadir/verificar permisos del worker para leer, reclamar y actualizar la outbox.
- No crear una cola bridge ni una suscripción SNS para Kafka.
- No habilitar DynamoDB Streams.

### Fase 3: Kafka local KRaft

- Crear Docker Compose sin ZooKeeper.
- Probar PLAINTEXT en una red privada de desarrollo.
- Crear explícitamente topics de negocio, DLT e internal topics con RF=3 y `min.insync.replicas=2` en el clúster objetivo.
- Probar productor, consumidor, particiones, offsets y DLT localmente.

### Fase 4: EC2 productiva

- Actualizar el stack existente tras el preflight: tres EC2 `t3.medium`, misma AZ, KRaft combinado.
- Crear un clúster nuevo sin migrar datos Kafka; aceptar la interrupción de la transición.
- Preservar el puerto 9092 y el CIDR verificado; anunciar las direcciones privadas de los brokers.
- Replicar el almacenamiento cifrado/persistente confirmado en preflight y validar capacidad.
- Crear topics y verificar RF/minISR y persistencia tras reinicio.

### Fase 5: worker de outbox y conectividad

- Mantener el worker de polling existente; no implementar SNS/SQS -> Kafka ni DynamoDB Streams.
- Configurar los tres endpoints privados en `KAFKA_BROKERS`.
- Publicar con `acks=all`; marcar como publicado solo tras el ack Kafka.
- Probar leases, reintentos, duplicados y recuperación del backlog tras el retorno del worker.

### Fase 6: dual-publish y outbox

- Añadir outbox a `createOrder` y `changeOrderStatus`.
- Mantener SNS funcionando.
- Procesar reintentos de outbox.
- Probar caída de Kafka sin perder el evento de negocio.

### Fase 7: consumidores Kafka

- Implementar `report-service` y `audit-service`.
- Configurar grupos independientes.
- Validar offsets, commits, reinicios e idempotencia.
- Mantener email/shipping/status sobre SQS.

### Fase 7.1: analítica en tiempo real mediante consumer Kafka

Kafka no mostrará analíticas por sí solo: entregará el flujo continuo a consumidores
especializados. Implementar un `analytics-service` con su propio consumer group:

- Consumir `orders.events` continuamente.
- Procesar `ORDER_CREATED` y `ORDER_STATUS_CHANGED`.
- Confirmar offsets únicamente después de actualizar las métricas.
- Hacer las actualizaciones idempotentes usando `eventId`.
- Calcular métricas como pedidos por minuto, ventas acumuladas, pedidos por estado y eventos procesados.
- Persistir agregaciones en una tabla DynamoDB de analítica o el almacenamiento que se decida para consultas.
- Exponer endpoints de lectura en el backend para consultar las métricas.
- Añadir alarmas para lag del consumer group y errores de procesamiento.
- Dejar el dashboard del frontend como una fase posterior que consuma esos endpoints.

Este consumer de Kafka no es DynamoDB Streams; la publicación desde outbox sigue usando polling.

### Fase 8: CI/CD y operación

- Añadir workflow de build, publicación y SSM.
- Incorporar healthchecks, alarmas y rollback.
- Probar una actualización controlada y la recuperación.

## 15. Criterios de aceptación

- Kafka funciona en modo KRaft sin ZooKeeper.
- Existen exactamente tres EC2 `t3.medium` activas en la misma AZ, cada una con broker/controller KRaft combinado.
- El clúster se crea limpio, sin migración de datos Kafka, y la interrupción de transición está aceptada.
- El layout de almacenamiento cifrado/persistente coincide con el verificado en preflight y tiene capacidad validada.
- `orders.events` tiene tres particiones y `orders.events.dlt` una.
- Topics de negocio, DLT e internal topics tienen RF=3 y `min.insync.replicas=2`.
- El productor usa `acks=all`.
- `orderId` determina la partición.
- Los offsets se confirman después del procesamiento.
- Un evento se entrega tanto a SNS/SQS como a Kafka.
- Kafka se publica solo por el worker que consulta la outbox; no hay ruta target de bridge ni DynamoDB Streams.
- Se conserva el puerto 9092 y el CIDR verificado; los brokers anuncian IPs privadas y los workers tienen tres endpoints privados de bootstrap.
- El PoC usa PLAINTEXT solo en la red privada; TLS/SASL queda diferido.
- La caída de Kafka no pierde el evento gracias a la outbox.
- Los duplicados no producen efectos de negocio duplicados.
- Una prueba de carga procesa 5.000 eventos válidos (`LOAD_TEST_TOTAL=5000`, con contadores DLT/retry en `0`) y una prueba negativa pequeña separada (por ejemplo, 10 reintentables y 15 DLT).
- La limpieza del script borra solo los registros de outbox del run; documentar la persistencia de mensajes Kafka/DLT y métricas Analytics.
- Con el clúster sano se detiene una EC2 completa: verificar desde un cliente fuera de esa EC2 que Kafka siga disponible; al volver la EC2, el worker publica el backlog de outbox.
- Las pruebas y la limpieza de sus datos no eliminan las tres EC2 ni sus volúmenes.
- Un update CloudFormation puede reemplazar la EC2 física original; el estado final contiene exactamente tres EC2.
- El `analytics-service` consume el stream continuamente con un consumer group independiente.
- Las métricas se actualizan de forma idempotente y pueden consultarse desde el backend.
- El lag y los errores del consumidor de analítica generan alarmas.
- El preflight registra los parámetros y recursos vivos del stack, región, red, CIDR y almacenamiento antes de cualquier update.
- No hay secretos en Git, imágenes ni logs.
- CloudWatch alerta ante lag, DLQ, disco y caída de servicios.
- GitHub Actions puede actualizar y hacer rollback mediante SSM.
- El despliegue se detiene claramente si `LabRole` no tiene permisos suficientes.

## 16. Riesgos explícitos

1. **Tres nodos en una sola AZ**: una falla de EC2 se prueba con RF=3/minISR=2; la disponibilidad ante una falla de AZ no forma parte de este objetivo.
2. **Red/PLAINTEXT del PoC**: el listener debe permanecer limitado a la red privada y al CIDR verificado; TLS/SASL se difiere.
3. **CloudFormation puede reemplazar la EC2 física original**: se acepta downtime; revisar retención de EC2/EBS y confirmar exactamente tres EC2 al final.
4. **AWS Academy/LabRole**: puede bloquear EC2, EBS, SSM, CloudWatch o recursos de IAM.
5. **Dual-publish**: requiere outbox e idempotencia; dos destinos no forman una transacción atómica.
6. **Actualización destructiva de EC2**: puede interrumpir Kafka; rollback y snapshots son obligatorios.
7. **Retención de 7 días**: no sustituye un backup; los snapshots y la DLT tienen objetivos distintos.

## 17. Resultado esperado

La entrega objetivo es un clúster nuevo de tres EC2 `t3.medium` KRaft combinado en la misma AZ, con polling de outbox, RF=3/minISR=2 y endpoints privados. Validar primero el clúster sano y la carga de 5.000 eventos válidos; después hacer el fallo controlado de una EC2 completa y comprobar disponibilidad Kafka y recuperación del backlog al retornar. No migrar datos Kafka del clúster anterior; se acepta downtime durante la transición.

## 18. Progreso de implementación

### Completado

Las casillas siguientes describen componentes/pruebas ya existentes, no cambian la ruta objetivo: el bridge SQS existente no será el camino de publicación Kafka; el worker de polling de outbox sí lo será. El PoC objetivo usa PLAINTEXT privado y no DynamoDB Streams.

- [x] Definición del plan y decisiones de arquitectura.
- [x] Contrato común de eventos validado con Zod.
- [x] Publicación SNS usando el contrato común.
- [x] Tests unitarios del contrato y regresión del conjunto existente.
- [x] Cola SQS dedicada para el bridge con long polling.
- [x] DLQ dedicada del bridge con tres reintentos.
- [x] Suscripción SNS de la cola bridge para `ORDER_CREATED` y `ORDER_STATUS_CHANGED`.
- [x] Tabla DynamoDB outbox con índice `StatusCreatedAtIndex`.
- [x] Variable `OUTBOX_TABLE` disponible para las Lambdas.
- [x] `createOrder` registra `ORDER_CREATED` en la outbox dentro de su transacción.
- [x] `changeOrderStatus` registra `ORDER_STATUS_CHANGED` en la misma transacción del cambio.
- [x] Los eventos SNS reutilizan el mismo envelope persistido en la outbox.
- [x] Tests unitarios de creación y validación de registros outbox.
- [x] Decisión de una sola ruta Kafka: outbox con polling como ruta de publicación; no se implementa un bridge SNS/SQS.
- [x] Worker inicial de outbox con KafkaJS, SASL/SCRAM, TLS, key `orderId`, leases y reintentos.
- [x] Docker Compose local con un broker Kafka 3.9.0 en modo KRaft, sin ZooKeeper.
- [x] Creación explícita de `orders.events` y `orders.events.dlt` con particiones y retención.
- [x] Broker local verificado como `healthy`.
- [x] Publicación y consumo local de un evento con `orderId` como key.
- [x] Evento conservado después de reiniciar el contenedor Kafka.
- [x] Tabla DynamoDB de analítica parametrizada como `ANALYTICS_TABLE`.
- [x] Consumer group `analytics-service` y agregaciones idempotentes por `eventId`.
- [x] Eventos inválidos del consumidor se publican en `orders.events.dlt` sin bloquear el consumer group.
- [x] Tests de buckets por minuto, ventas y cambios de estado.
- [x] Endpoint `GET /api/report/analytics` para consultar métricas por rango.
- [x] DynamoDB Local en memoria disponible en el Compose para pruebas end-to-end de analítica.
- [x] Prueba end-to-end Kafka -> `analytics-service` -> DynamoDB Local.
- [x] Duplicados por `eventId` verificados sin incrementar las métricas.
- [x] Dockerfile común para `outboxPublisher` y `analytics-service`.
- [x] Servicios Compose de workers aislados bajo el perfil `workers`.
- [x] Imagen de workers construida y conexión interna verificada mediante `kafka:29092`.
- [x] El código de workers permite TLS/SASL configurable; para el PoC objetivo se difiere y se usa PLAINTEXT privado según §9.
- [x] Workflow manual para publicar workers en GHCR y actualizar EC2 mediante SSM.
- [x] Workflow espera el resultado del despliegue SSM y muestra stdout/stderr ante errores.

### Pendiente inmediato

- [ ] Verificar permisos IAM mínimos para polling/actualización de outbox y operación de workers cuando se confirme lo permitido por `LabRole`.
- [x] Workflow de aprovisionamiento CloudFormation para EC2, EIP, Security Group y EBS.
- [ ] Validar en la cuenta AWS los permisos SSM/EC2 del `LabRole` y configurar los secrets del workflow.
- [ ] Añadir tests de outbox, reintentos e idempotencia.
- [x] Recuperar leases `PROCESSING` expirados tras una caída del worker.
- [x] Levantar el broker local y verificar healthcheck, particiones y retención.
- [x] Conectar el frontend a esos endpoints cuando el backend esté validado.
