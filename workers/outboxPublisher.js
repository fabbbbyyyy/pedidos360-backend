const fs = require('fs');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, QueryCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { Kafka } = require('kafkajs');

const required = (name) => {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} no está configurado`);
  }
  return value;
};

const tableName = required('OUTBOX_TABLE');
const region = required('AWS_REGION');
const batchSize = Number(process.env.OUTBOX_BATCH_SIZE || 25);
const leaseSeconds = Number(process.env.OUTBOX_LEASE_SECONDS || 60);
const pollMilliseconds = Number(process.env.OUTBOX_POLL_MS || 5000);
const topic = process.env.KAFKA_TOPIC || 'orders.events';
const kafkaTls = process.env.KAFKA_TLS !== 'false';

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({
  region,
  endpoint: process.env.DYNAMODB_ENDPOINT || undefined,
  credentials: process.env.DYNAMODB_ENDPOINT ? {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'local',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'local',
  } : undefined,
}));
const kafkaOptions = {
  clientId: process.env.KAFKA_CLIENT_ID || 'pedidos360-outbox-publisher',
  brokers: required('KAFKA_BROKERS').split(',').map((broker) => broker.trim()),
  retry: { retries: 5 },
};

if (kafkaTls) {
  kafkaOptions.ssl = {
    ca: [fs.readFileSync(required('KAFKA_CA_PATH'), 'utf8')],
  };
}

if (process.env.KAFKA_SASL_USERNAME) {
  kafkaOptions.sasl = {
    mechanism: process.env.KAFKA_SASL_MECHANISM || 'scram-sha-512',
    username: process.env.KAFKA_SASL_USERNAME,
    password: required('KAFKA_SASL_PASSWORD'),
  };
}

const kafka = new Kafka(kafkaOptions);
const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 1,
});

const queryByStatus = async (status) => {
  const result = await dynamo.send(new QueryCommand({
    TableName: tableName,
    IndexName: 'StatusCreatedAtIndex',
    KeyConditionExpression: 'gsi1pk = :status',
    ExpressionAttributeValues: { ':status': `STATUS#${status}` },
    ScanIndexForward: true,
    Limit: batchSize,
  }));
  return result.Items || [];
};

const recoverExpiredLeases = async () => {
  const now = Math.floor(Date.now() / 1000);
  const result = await dynamo.send(new QueryCommand({
    TableName: tableName,
    IndexName: 'StatusCreatedAtIndex',
    KeyConditionExpression: 'gsi1pk = :status',
    FilterExpression: 'leaseUntil <= :now',
    ExpressionAttributeValues: {
      ':status': 'STATUS#PROCESSING',
      ':now': now,
    },
    ScanIndexForward: true,
    Limit: batchSize,
  }));

  for (const item of result.Items || []) {
    await dynamo.send(new UpdateCommand({
      TableName: tableName,
      Key: { pk: item.pk, sk: item.sk },
      UpdateExpression: 'SET #status = :pending, gsi1pk = :pendingIndex, updatedAt = :updatedAt REMOVE leaseUntil',
      ConditionExpression: '#status = :processing AND leaseUntil <= :now',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: {
        ':processing': 'PROCESSING',
        ':pending': 'PENDING',
        ':pendingIndex': 'STATUS#PENDING',
        ':updatedAt': new Date().toISOString(),
        ':now': now,
      },
    }));
  }
};

const claim = async (item) => {
  const leaseUntil = Math.floor(Date.now() / 1000) + leaseSeconds;
  const result = await dynamo.send(new UpdateCommand({
    TableName: tableName,
    Key: { pk: item.pk, sk: item.sk },
    UpdateExpression: 'SET #status = :processing, gsi1pk = :processingIndex, leaseUntil = :leaseUntil, updatedAt = :updatedAt',
    ConditionExpression: '#status = :pending',
    ExpressionAttributeNames: { '#status': 'status' },
    ExpressionAttributeValues: {
      ':pending': 'PENDING',
      ':processing': 'PROCESSING',
      ':processingIndex': 'STATUS#PROCESSING',
      ':leaseUntil': leaseUntil,
      ':updatedAt': new Date().toISOString(),
    },
    ReturnValues: 'ALL_NEW',
  }));
  return result.Attributes;
};

const publish = async (item) => {
  await producer.send({
    topic,
    messages: [{
      key: item.data.orderId,
      value: JSON.stringify({
        eventId: item.eventId,
        eventType: item.eventType,
        source: item.source,
        occurredAt: item.occurredAt,
        data: item.data,
      }),
      headers: { eventId: item.eventId, eventType: item.eventType },
    }],
  });
};

const markPublished = (item) => dynamo.send(new UpdateCommand({
  TableName: tableName,
  Key: { pk: item.pk, sk: item.sk },
  UpdateExpression: 'SET #status = :published, gsi1pk = :publishedIndex, publishedAt = :publishedAt REMOVE leaseUntil',
  ConditionExpression: '#status = :processing',
  ExpressionAttributeNames: { '#status': 'status' },
  ExpressionAttributeValues: {
    ':processing': 'PROCESSING',
    ':published': 'PUBLISHED',
    ':publishedIndex': 'STATUS#PUBLISHED',
    ':publishedAt': new Date().toISOString(),
  },
}));

const markPending = (item, error) => dynamo.send(new UpdateCommand({
  TableName: tableName,
  Key: { pk: item.pk, sk: item.sk },
  UpdateExpression: 'SET #status = :pending, gsi1pk = :pendingIndex, attempts = if_not_exists(attempts, :zero) + :one, lastError = :lastError, updatedAt = :updatedAt REMOVE leaseUntil',
  ConditionExpression: '#status = :processing',
  ExpressionAttributeNames: { '#status': 'status' },
  ExpressionAttributeValues: {
    ':processing': 'PROCESSING',
    ':pending': 'PENDING',
    ':pendingIndex': 'STATUS#PENDING',
    ':zero': 0,
    ':one': 1,
    ':lastError': error.message,
    ':updatedAt': new Date().toISOString(),
  },
}));

const processBatch = async () => {
  await recoverExpiredLeases();
  const pending = await queryByStatus('PENDING');
  for (const item of pending) {
    let claimed;
    try {
      claimed = await claim(item);
      await publish(claimed);
      await markPublished(claimed);
    } catch (error) {
      if (claimed) {
        await markPending(claimed, error);
      }
      console.error('No se pudo publicar evento de outbox', {
        eventId: item.eventId,
        error: error.message,
      });
    }
  }
};

const run = async () => {
  await producer.connect();
  console.log('Outbox publisher conectado a Kafka');
  while (true) {
    await processBatch();
    await new Promise((resolve) => setTimeout(resolve, pollMilliseconds));
  }
};

if (require.main === module) {
  run().catch((error) => {
    console.error('Outbox publisher detenido', error);
    process.exitCode = 1;
  });
}

module.exports = {
  recoverExpiredLeases,
  processBatch,
};
