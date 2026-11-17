const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');

const total = Number(process.env.LOAD_TEST_TOTAL || 100);
const dltCount = Number(process.env.LOAD_TEST_DLT_COUNT || 15);
const retryCount = Number(process.env.LOAD_TEST_RETRY_COUNT || 10);
const mode = process.env.LOAD_TEST_MODE || 'insert';
const tableName = process.env.OUTBOX_TABLE;
const region = process.env.AWS_REGION || 'us-east-1';

if (!tableName) {
  throw new Error('OUTBOX_TABLE es obligatorio');
}
if (!Number.isInteger(total) || total < 1) {
  throw new Error('LOAD_TEST_TOTAL debe ser un entero positivo');
}
if (!Number.isInteger(dltCount) || dltCount < 0 || !Number.isInteger(retryCount) || retryCount < 0) {
  throw new Error('LOAD_TEST_DLT_COUNT y LOAD_TEST_RETRY_COUNT deben ser enteros no negativos');
}
if (dltCount + retryCount > total) {
  throw new Error('La suma de LOAD_TEST_DLT_COUNT y LOAD_TEST_RETRY_COUNT no puede superar LOAD_TEST_TOTAL');
}
if (!['insert', 'cleanup'].includes(mode)) {
  throw new Error('LOAD_TEST_MODE debe ser insert o cleanup');
}

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({ region }));
const runId = process.env.LOAD_TEST_RUN_ID || `load-test-${Date.now()}`;

const buildItem = (index) => {
  const eventId = `${runId}-${index}`;
  const orderId = `${runId}-order-${index}`;
  const occurredAt = new Date(Date.now() + index).toISOString();
  const retry = index <= retryCount;
  const dlt = !retry && index <= retryCount + dltCount;
  const eventType = index % 2 === 0 ? 'ORDER_STATUS_CHANGED' : 'ORDER_CREATED';
  const event = {
    eventId,
    eventType,
    source: 'load-test',
    occurredAt,
    data: {
      orderId,
      ...(eventType === 'ORDER_STATUS_CHANGED'
        ? { previousStatus: 'PENDING', status: 'CONFIRMED', version: 2 }
        : { status: 'PENDING', total: 1000 + index }),
      total: 1000 + index,
    },
  };

  if (dlt) delete event.eventType;
  if (retry) delete event.data;

  return {
    pk: `EVENT#${eventId}`,
    sk: `EVENT#${eventId}`,
    entityType: 'OUTBOX_EVENT',
    ...event,
    status: 'PENDING',
    attempts: 0,
    gsi1pk: 'STATUS#PENDING',
    gsi1sk: `${occurredAt}#${eventId}`,
  };
};

const chunks = (items, size) => {
  const result = [];
  for (let index = 0; index < items.length; index += size) {
    result.push(items.slice(index, index + size));
  }
  return result;
};

const writeBatch = async (items) => {
  let pending = items.map((Item) => ({ PutRequest: { Item } }));
  while (pending.length > 0) {
    const result = await dynamo.send(new BatchWriteCommand({
      RequestItems: { [tableName]: pending },
    }));
    pending = result.UnprocessedItems?.[tableName] || [];
    if (pending.length > 0) await new Promise((resolve) => setTimeout(resolve, 250));
  }
};

const deleteBatch = async (items) => {
  let pending = items.map((item) => ({
    DeleteRequest: { Key: { pk: item.pk, sk: item.sk } },
  }));
  while (pending.length > 0) {
    const result = await dynamo.send(new BatchWriteCommand({
      RequestItems: { [tableName]: pending },
    }));
    pending = result.UnprocessedItems?.[tableName] || [];
    if (pending.length > 0) await new Promise((resolve) => setTimeout(resolve, 250));
  }
};

const run = async () => {
  const items = Array.from({ length: total }, (_, index) => buildItem(index + 1));
  if (mode === 'cleanup') {
    if (!process.env.LOAD_TEST_RUN_ID) {
      throw new Error('LOAD_TEST_RUN_ID es obligatorio cuando LOAD_TEST_MODE=cleanup');
    }
    for (const batch of chunks(items, 25)) await deleteBatch(batch);
    console.log(JSON.stringify({ mode, runId, tableName, deleted: items.length }));
    return;
  }

  for (const batch of chunks(items, 25)) await writeBatch(batch);
  console.log(JSON.stringify({
    mode,
    runId,
    tableName,
    total,
    valid: total - dltCount - retryCount,
    dlt: dltCount,
    retry: retryCount,
  }));
};

run().catch((error) => {
  console.error('No se pudo ejecutar la carga de prueba', error);
  process.exitCode = 1;
});
