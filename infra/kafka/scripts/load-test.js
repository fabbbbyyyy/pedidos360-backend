const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');

const total = Number(process.env.LOAD_TEST_TOTAL || 500);
const invalidEvery = Number(process.env.LOAD_TEST_INVALID_EVERY || 5);
const tableName = process.env.OUTBOX_TABLE;
const region = process.env.AWS_REGION || 'us-east-1';

if (!tableName) {
  throw new Error('OUTBOX_TABLE es obligatorio');
}
if (!Number.isInteger(total) || total < 1) {
  throw new Error('LOAD_TEST_TOTAL debe ser un entero positivo');
}
if (!Number.isInteger(invalidEvery) || invalidEvery < 0) {
  throw new Error('LOAD_TEST_INVALID_EVERY debe ser un entero mayor o igual a cero');
}

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({ region }));
const runId = `load-test-${Date.now()}`;

const buildItem = (index) => {
  const eventId = `${runId}-${index}`;
  const orderId = `${runId}-order-${index}`;
  const occurredAt = new Date(Date.now() + index).toISOString();
  const invalid = invalidEvery > 0 && index % invalidEvery === 0;
  const event = {
    eventId,
    eventType: 'ORDER_CREATED',
    source: 'load-test',
    occurredAt,
    data: {
      orderId,
      status: 'PENDING',
      total: 1000 + index,
    },
  };

  if (invalid) delete event.eventType;

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

const run = async () => {
  const items = Array.from({ length: total }, (_, index) => buildItem(index + 1));
  for (const batch of chunks(items, 25)) await writeBatch(batch);

  const invalid = invalidEvery > 0 ? Math.floor(total / invalidEvery) : 0;
  console.log(JSON.stringify({
    runId,
    tableName,
    total,
    valid: total - invalid,
    invalid,
    invalidEvery,
  }));
};

run().catch((error) => {
  console.error('No se pudo ejecutar la carga de prueba', error);
  process.exitCode = 1;
});
