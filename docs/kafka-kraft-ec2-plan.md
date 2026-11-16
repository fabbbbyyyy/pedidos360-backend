# Plan de implementación: Kafka KRaft en EC2

## 1. Objetivo y decisiones confirmadas

Integrar Kafka con KRaft en una EC2 propia, ejecutado con Docker, sin retirar SNS/SQS:

- SNS/SQS y Kafka coexistirán permanentemente.
- Cada evento se entregará a ambos sistemas.
- Kafka tendrá un único broker KRaft en producción inicialmente.
- El bridge SNS/SQS -> Kafka correrá en un contenedor separado en la misma EC2.
- Los productores evolucionarán a dual-publish mediante una outbox en DynamoDB.
- La entrega será **at-least-once**; los consumidores serán idempotentes.
- Se usarán topics de dominio, tres particiones iniciales y `orderId` como clave.
- Se usará `orders.events` y `orders.events.dlt`.
- Retención: 7 días para eventos normales y 30 días para DLT.
- TLS con CA privada autofirmada, SAN para la Elastic IP y validación estricta.
- SASL/SCRAM para autenticar productores y consumidores.
- La EC2 usará Amazon Linux, Docker Compose y una Elastic IP.
- La administración será exclusivamente mediante SSM o VPN.
- CloudWatch recibirá logs, métricas y alarmas.
- GitHub Actions ejecutará actualizaciones mediante SSM, aceptando una caída durante el despliegue.
- Se reutilizará exclusivamente el `LabRole` existente; no se presupone la creación de roles IAM nuevos.

## 2. Estado actual y punto de integración

El backend actual ya publica:

- `ORDER_CREATED` desde `createOrder`.
- `ORDER_STATUS_CHANGED` desde `changeOrderStatus`.

SNS distribuye los mensajes a tres colas SQS, cada una con su DLQ:

- Email.
- Shipping.
- Status.

La primera integración no debe reutilizar esas colas, porque quitaría mensajes a los consumidores existentes. Se añadirá una cola SQS y una DLQ dedicadas para el bridge, suscritas al mismo topic SNS.

El proyecto de referencia contiene topics, grupos de consumidores, reintentos y DLT, pero usa ZooKeeper y no implementa el consumo real de las colas del backend. Se reutilizarán sus ideas, no su configuración productiva.

## 3. Arquitectura objetivo

```text
                    +---------------------+
                    |  Lambda createOrder |
                    |  Lambda status      |
                    +----------+----------+
                               |
                 +-------------+-------------+
                 |                           |
             SNS topic                  DynamoDB outbox
                 |                           |
        +--------+--------+                  |
        |                 |                  |
   SQS existentes    SQS bridge               |
        |                 |                  |
 email/shipping/status  EC2                  |
                       |                     |
                bridge container             |
                       |                     |
                Kafka KRaft broker           |
                       |                     |
                 Kafka consumers             |
```

La EC2 tendrá dos servicios principales:

1. Broker Kafka KRaft con volumen EBS dedicado.
2. Bridge que consume la cola SQS dedicada y publica en Kafka.

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

| Topic | Particiones | Retención | Uso |
|---|---:|---:|---|
| `orders.events` | 3 | 7 días | Eventos de pedidos |
| `orders.events.dlt` | 1 | 30 días | Mensajes que agotaron reintentos |

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
- El bridge debe soportar reintentos del mismo mensaje SQS.
- Los consumidores deben registrar eventos procesados o usar una operación idempotente en su almacenamiento.
- Kafka producer debe usar `acks=all`, `enable.idempotence=true` y reintentos limitados.

## 7. Bridge SNS/SQS -> Kafka

Implementar el bridge como servicio Node.js separado, reutilizando KafkaJS y los validadores del repositorio cuando sea posible.

Responsabilidades:

1. Consumir la cola SQS dedicada mediante long polling.
2. Parsear y validar el envelope.
3. Publicar en `orders.events` con `orderId` como key.
4. Confirmar el mensaje SQS solo después del ack de Kafka.
5. Reintentar errores transitorios con backoff.
6. Enviar errores permanentes a la DLQ de SQS o a `orders.events.dlt`, según el punto de fallo.
7. Exponer métricas de mensajes procesados, errores y latencia.

No usar una de las colas existentes como bridge.

## 8. Kafka KRaft en Docker

Crear artefactos versionados, sin ZooKeeper:

```text
infra/kafka/
  docker-compose.yml
  .env.example
  config/
  scripts/
    generate-ca.sh
    generate-cert.sh
    create-topics.sh
  README.md
bridge/
  src/
  Dockerfile
```

El compose debe fijar versiones, nunca usar `latest`, y configurar:

- `process.roles=broker,controller`.
- `node.id` estable.
- `controller.quorum.voters` para el nodo único.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`.
- Listener interno de controller y listener externo TLS/SASL.
- Volumen persistente para `/var/lib/kafka/data`.
- Healthcheck del broker.
- Límites de memoria y disco.
- Reinicio automático del contenedor.

El nodo único es una decisión de disponibilidad, no de durabilidad. La durabilidad dependerá del volumen EBS, snapshots y restauración probada.

## 9. TLS, SASL y secretos

### TLS

Generar una CA privada y un certificado cuyo SAN incluya la Elastic IP. Distribuir la CA a los clientes Lambda y al bridge. Todos los clientes deben validar el certificado; no se debe desactivar `rejectUnauthorized`.

### SASL

Configurar SASL/SCRAM con credenciales separadas para:

- bridge producer.
- `report-service`.
- `audit-service`.
- administración.

### Restricción de LabRole

El repositorio reutiliza `AWS_ROLE_ARN`/`LabRole` porque AWS Academy puede impedir crear roles. Antes de implementar, comprobar que el rol existente puede:

- Crear y administrar EC2, EBS, Elastic IP, Security Groups y CloudWatch.
- Ejecutar comandos por SSM.
- Leer o escribir la configuración que se decida usar.
- Permitir a las Lambdas acceder a los recursos necesarios.

Si `LabRole` no tiene estos permisos, el despliegue debe detenerse con un diagnóstico. No se debe compensar desactivando TLS, exponiendo secretos en el repositorio o concediendo permisos amplios.

## 10. EC2 y red

### EC2

- Amazon Linux 2023.
- Tipo de instancia parametrizado y elegido inicialmente por costo.
- EBS cifrado separado para datos Kafka.
- Elastic IP asociada.
- Docker y Docker Compose instalados por User Data o script versionado.
- SSM Agent habilitado.
- Sin SSH público.

### Security Group

Permitir únicamente:

- Puerto Kafka TLS/SASL desde las fuentes autorizadas de Lambda.
- Puerto de SSM según la configuración de la cuenta.
- Sin exponer Kafka UI ni endpoints administrativos a Internet.

La conectividad directa desde Lambda a una IP pública debe probarse con la configuración de red real de la cuenta. Las reglas no deben abrir Kafka a `0.0.0.0/0` salvo una restricción temporal explícita y documentada.

## 11. Serverless y CloudFormation

Extender `serverless.yml` y los recursos separados para parametrizar:

- Cola y DLQ del bridge.
- Suscripción SNS de la cola bridge.
- Tabla DynamoDB outbox.
- Variables de entorno de topics, broker y consumer groups.
- Elastic IP y Security Group solo si los permisos de `LabRole` lo permiten.
- Alarmas y log groups de CloudWatch.

No mezclar secretos privados con valores versionados. El plan debe respetar el mecanismo actual de `.env` para configuración no sensible, pero la CA y credenciales no deben terminar en Git ni en el bundle de Lambda.

## 12. Despliegue con GitHub Actions y SSM

Fases del workflow:

1. Instalar dependencias y ejecutar tests.
2. Validar formato, configuración Kafka y archivos Compose.
3. Construir y fijar la imagen del bridge.
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
- Verificación posterior de broker, bridge, topics y lag.

## 13. Observabilidad y operación

Enviar a CloudWatch:

- Logs de broker.
- Logs del bridge.
- Logs de consumidores.
- Errores de publicación.
- Mensajes en DLQ.
- Consumer lag.
- Uso de CPU, memoria y disco.
- Estado del volumen EBS.

Alarmas mínimas:

- Kafka no saludable.
- Disco por encima del umbral.
- Bridge detenido.
- Mensajes en DLQ.
- Lag creciente.
- Fallos de publicación de la outbox.
- EC2 sin heartbeat de SSM.

Procedimientos documentados:

- Reiniciar bridge.
- Reiniciar broker.
- Restaurar snapshot EBS.
- Reprocesar DLT.
- Rotar credenciales SASL.
- Rotar certificado/CA.
- Cambiar la Elastic IP si se pierde.
- Hacer rollback del despliegue.

## 14. Orden de implementación por fases

### Fase 0: permisos y prueba mínima

- Confirmar qué permite exactamente `LabRole`.
- Confirmar región, VPC, subredes, límites de EC2/EIP/EBS y SSM.
- Verificar que Lambda puede conectar a la Elastic IP.
- No avanzar si no se puede desplegar la infraestructura mínima con los permisos disponibles.

### Fase 1: contrato y módulos compartidos

- Extraer envelope y validadores comunes.
- Añadir tests para `eventId`, `eventType`, `occurredAt`, `orderId` y versionado.
- Definir configuración de topics y consumer groups.

### Fase 2: infraestructura AWS del bridge

- Crear cola y DLQ dedicadas.
- Suscribirlas a SNS con los filtros de eventos adecuados.
- Crear tabla outbox.
- Añadir permisos y variables de entorno.
- Probar SNS -> SQS sin Kafka.

### Fase 3: Kafka local KRaft

- Crear Docker Compose sin ZooKeeper.
- Generar CA/certificados de desarrollo.
- Crear topics explícitamente.
- Probar productor, consumidor, particiones, offsets y DLT localmente.

### Fase 4: EC2 productiva

- Provisionar EC2, EBS, Elastic IP, Security Group y SSM.
- Instalar Docker.
- Levantar Kafka KRaft.
- Verificar TLS/SASL desde un cliente externo autorizado.
- Crear topics y comprobar persistencia tras reinicio.

### Fase 5: bridge productivo

- Implementar el consumidor SQS.
- Publicar en Kafka con confirmación.
- Confirmar SQS únicamente después del ack.
- Probar reintentos, DLQ, reinicio y duplicados.

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

### Fase 7.1: streaming y analítica en tiempo real

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

### Fase 8: CI/CD y operación

- Añadir workflow de build, publicación y SSM.
- Incorporar healthchecks, alarmas y rollback.
- Probar una actualización controlada y la recuperación.

## 15. Criterios de aceptación

- Kafka funciona en modo KRaft sin ZooKeeper.
- El broker conserva datos después de reiniciar el contenedor y la EC2.
- `orders.events` tiene tres particiones y `orders.events.dlt` una.
- `orderId` determina la partición.
- Los offsets se confirman después del procesamiento.
- Un evento se entrega tanto a SNS/SQS como a Kafka.
- El bridge no consume las colas de email/shipping/status.
- La caída de Kafka no pierde el evento gracias a la outbox.
- Los duplicados no producen efectos de negocio duplicados.
- El `analytics-service` consume el stream continuamente con un consumer group independiente.
- Las métricas se actualizan de forma idempotente y pueden consultarse desde el backend.
- El lag y los errores del consumidor de analítica generan alarmas.
- TLS valida la CA privada y el SAN de la Elastic IP.
- SASL rechaza credenciales inválidas.
- No hay secretos en Git, imágenes ni logs.
- CloudWatch alerta ante lag, DLQ, disco y caída de servicios.
- GitHub Actions puede actualizar y hacer rollback mediante SSM.
- El despliegue se detiene claramente si `LabRole` no tiene permisos suficientes.

## 16. Riesgos explícitos

1. **Un único broker en producción**: no hay tolerancia a fallo del nodo. El plan debe dejar preparada la migración a tres brokers KRaft.
2. **IP pública**: aumenta la superficie de ataque y requiere reglas estrictas y Elastic IP.
3. **CA autofirmada**: exige distribución y rotación manual de confianza.
4. **AWS Academy/LabRole**: puede bloquear EC2, EIP, SSM, CloudWatch o recursos de IAM.
5. **Dual-publish**: requiere outbox e idempotencia; dos destinos no forman una transacción atómica.
6. **Actualización destructiva de EC2**: puede interrumpir Kafka; rollback y snapshots son obligatorios.
7. **Retención de 7 días**: no sustituye un backup; los snapshots y la DLT tienen objetivos distintos.

## 17. Resultado esperado

La primera entrega debe ser un entorno local KRaft reproducible, una prueba de integración
SNS/SQS + outbox -> Kafka y un consumidor de analítica que actualice métricas en tiempo real.
Solo después de validar contrato, seguridad, offsets, reintentos, persistencia y agregaciones
se debe desplegar en la EC2 de producción.

## 18. Progreso de implementación

### Completado

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
- [x] Decisión de una sola ruta Kafka: outbox principal; bridge solo para migración/replay controlado.
- [x] Worker inicial de outbox con KafkaJS, SASL/SCRAM, TLS, key `orderId`, leases y reintentos.
- [x] Docker Compose local con un broker Kafka 3.9.0 en modo KRaft, sin ZooKeeper.
- [x] Creación explícita de `orders.events` y `orders.events.dlt` con particiones y retención.
- [x] Broker local verificado como `healthy`.
- [x] Publicación y consumo local de un evento con `orderId` como key.
- [x] Evento conservado después de reiniciar el contenedor Kafka.
- [x] Tabla DynamoDB de analítica parametrizada como `ANALYTICS_TABLE`.
- [x] Consumer group `analytics-service` y agregaciones idempotentes por `eventId`.
- [x] Tests de buckets por minuto, ventas y cambios de estado.
- [x] Endpoint `GET /api/report/analytics` para consultar métricas por rango.
- [x] DynamoDB Local en memoria disponible en el Compose para pruebas end-to-end de analítica.
- [x] Prueba end-to-end Kafka -> `analytics-service` -> DynamoDB Local.
- [x] Duplicados por `eventId` verificados sin incrementar las métricas.
- [x] Dockerfile común para `outboxPublisher` y `analytics-service`.
- [x] Servicios Compose de workers aislados bajo el perfil `workers`.
- [x] Imagen de workers construida y conexión interna verificada mediante `kafka:29092`.
- [x] Workers exigen TLS/SASL fuera del modo local explícito `KAFKA_TLS=false`.
- [x] Workflow manual para publicar workers en GHCR y actualizar EC2 mediante SSM.
- [x] Workflow espera el resultado del despliegue SSM y muestra stdout/stderr ante errores.

### Pendiente inmediato

- [ ] Añadir permisos IAM mínimos para Lambda/bridge cuando se confirme lo permitido por `LabRole`.
- [x] Workflow de aprovisionamiento CloudFormation para EC2, EIP, Security Group y EBS.
- [ ] Validar en la cuenta AWS los permisos SSM/EC2 del `LabRole` y configurar los secrets del workflow.
- [ ] Añadir tests de outbox, reintentos e idempotencia.
- [x] Recuperar leases `PROCESSING` expirados tras una caída del worker.
- [x] Levantar el broker local y verificar healthcheck, particiones y retención.
- [ ] Conectar el frontend a esos endpoints cuando el backend esté validado.
