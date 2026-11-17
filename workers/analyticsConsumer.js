const fs = require('fs');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, TransactWriteCommand } = require('@aws-sdk/lib-dynamodb');
const { Kafka } = require('kafkajs');
const { parseEvent } = require('../src/libs/messaging/eventContract');
const { buildAnalyticsTransaction } = require('./analyticsMetrics');

const required = (name) => {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} no está configurado`);
  }
  return value;
};

const tableName = required('ANALYTICS_TABLE');
const region = required('AWS_REGION');
const topic = process.env.KAFKA_TOPIC || 'orders.events';
const dltTopic = process.env.KAFKA_DLT_TOPIC || 'orders.events.dlt';
const groupId = process.env.KAFKA_ANALYTICS_GROUP || 'analytics-service';
const fromBeginning = process.env.KAFKA_ANALYTICS_FROM_BEGINNING === 'true';
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
  clientId: process.env.KAFKA_CLIENT_ID || 'pedidos360-analytics',
  brokers: required('KAFKA_BROKERS').split(',').map((broker) => broker.trim()),
};

if (kafkaTls) {
  kafkaOptions.ssl = {
    ca: [fs.readFileSync(required('KAFKA_CA_PATH'), 'utf8')],
  };
  kafkaOptions.sasl = {
    mechanism: process.env.KAFKA_SASL_MECHANISM || 'scram-sha-512',
    username: required('KAFKA_SASL_USERNAME'),
    password: required('KAFKA_SASL_PASSWORD'),
  };
}

const kafka = new Kafka(kafkaOptions);
const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false });
const dltProducer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });

const isTransactionConflict = (error) => error.name === 'TransactionCanceledException'
  && error.CancellationReasons?.some(({ Code }) => Code === 'TransactionConflict');

const isDuplicateEvent = (error) => error.name === 'TransactionCanceledException'
  && error.CancellationReasons?.some(({ Code }) => Code === 'ConditionalCheckFailed');

const buildDltMessage = ({ message, error, sourceTopic }) => ({
  key: message.key?.toString() || null,
  value: JSON.stringify({
    failedAt: new Date().toISOString(),
    reason: error.message,
    original: message.value?.toString() || '',
  }),
  headers: {
    error: error.message,
    sourceTopic,
  },
});

const publishToDlt = async ({ message, error }) => {
  await dltProducer.send({
    topic: dltTopic,
    acks: -1,
    messages: [buildDltMessage({ message, error, sourceTopic: topic })],
  });
};

const processMessage = async ({ message }) => {
  const event = parseEvent(message.value.toString());
  let conflictAttempt = 0;

  while (true) {
    try {
      await dynamo.send(new TransactWriteCommand({
        TransactItems: buildAnalyticsTransaction({ event, tableName }),
      }));
      return;
    } catch (error) {
      if (!isTransactionConflict(error)) {
        if (isDuplicateEvent(error)) {
          console.warn('Evento de analítica ya procesado', { eventId: event.eventId });
          return;
        }
        throw error;
      }

      conflictAttempt += 1;
      const backoffMs = Math.min(1000, 25 * (2 ** Math.min(conflictAttempt - 1, 5)));
      const delayMs = backoffMs + Math.floor(Math.random() * backoffMs);
      console.warn('Reintentando transacción de analytics por conflicto', {
        eventId: event.eventId,
        attempt: conflictAttempt,
        delayMs,
      });
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
};

const handleMessage = async ({ topic: messageTopic, partition, message }) => {
  try {
    await processMessage({ message });
  } catch (error) {
    await publishToDlt({ message, error });
    console.error('Evento enviado a la DLT de analytics', {
      topic: messageTopic,
      partition,
      offset: message.offset,
      error: error.message,
    });
  }

  await consumer.commitOffsets([{
    topic: messageTopic,
    partition,
    offset: (BigInt(message.offset) + 1n).toString(),
  }]);
};

const run = async () => {
  await dltProducer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning });
  console.log(`Analytics consumer conectado a ${topic} con group ${groupId}`);
  await consumer.run({
    autoCommit: false,
    eachMessage: handleMessage,
  });
};

const shutdown = async () => {
  await consumer.disconnect();
  await dltProducer.disconnect();
  process.exit(0);
};

process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);
if (require.main === module) {
  run().catch((error) => {
    console.error('Analytics consumer detenido', error);
    process.exitCode = 1;
  });
}

module.exports = { buildDltMessage, handleMessage, processMessage, publishToDlt };
