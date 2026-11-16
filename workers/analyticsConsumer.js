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

const processMessage = async ({ message }) => {
  const event = parseEvent(message.value.toString());
  try {
    await dynamo.send(new TransactWriteCommand({
      TransactItems: buildAnalyticsTransaction({ event, tableName }),
    }));
  } catch (error) {
    const isDuplicate = error.name === 'TransactionCanceledException'
      && error.CancellationReasons?.some(({ Code }) => Code === 'ConditionalCheckFailed');
    if (isDuplicate) {
      console.warn('Evento de analítica ya procesado', { eventId: event.eventId });
    } else {
      throw error;
    }
  }
};

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning });
  console.log(`Analytics consumer conectado a ${topic} con group ${groupId}`);
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic: messageTopic, partition, message }) => {
      await processMessage({ message });
      await consumer.commitOffsets([{
        topic: messageTopic,
        partition,
        offset: (BigInt(message.offset) + 1n).toString(),
      }]);
    },
  });
};

const shutdown = async () => {
  await consumer.disconnect();
  process.exit(0);
};

process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);
run().catch((error) => {
  console.error('Analytics consumer detenido', error);
  process.exitCode = 1;
});
