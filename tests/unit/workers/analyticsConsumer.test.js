jest.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: jest.fn(),
}));

jest.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: {
    from: jest.fn(() => ({ send: jest.fn() })),
  },
  TransactWriteCommand: jest.fn((input) => ({ input })),
}));

jest.mock('kafkajs', () => ({
  Kafka: jest.fn(() => ({
    consumer: jest.fn(() => ({
      connect: jest.fn(),
      subscribe: jest.fn(),
      run: jest.fn(),
      commitOffsets: jest.fn(),
      disconnect: jest.fn(),
    })),
    producer: jest.fn(() => ({
      connect: jest.fn(),
      send: jest.fn(),
      disconnect: jest.fn(),
    })),
  })),
}));

describe('analytics DLT messages', () => {
  let dynamo;
  let consumer;
  let dltProducerSend;

  beforeEach(() => {
    jest.resetModules();
    process.env.ANALYTICS_TABLE = 'analytics';
    process.env.AWS_REGION = 'us-east-1';
    process.env.KAFKA_BROKERS = 'localhost:9092';
    process.env.KAFKA_TLS = 'false';

    const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
    dynamo = { send: jest.fn() };
    DynamoDBDocumentClient.from.mockReturnValue(dynamo);
  });

  test('preserves the invalid payload and failure metadata', () => {
    const { buildDltMessage } = require('../../../workers/analyticsConsumer');
    const result = buildDltMessage({
      message: {
        key: Buffer.from('order-1'),
        value: Buffer.from('{"orderId":"order-1"}'),
      },
      error: new Error('eventType Required'),
      sourceTopic: 'orders.events',
    });
    const payload = JSON.parse(result.value);

    expect(result.key).toBe('order-1');
    expect(payload.original).toBe('{"orderId":"order-1"}');
    expect(payload.reason).toBe('eventType Required');
    expect(payload.failedAt).toEqual(expect.any(String));
    expect(result.headers.sourceTopic).toBe('orders.events');
  });

  test('publishes DLT messages with acknowledgements from all replicas', async () => {
    const { publishToDlt } = require('../../../workers/analyticsConsumer');
    const { Kafka } = require('kafkajs');
    const producerSend = Kafka.mock.results[0].value.producer.mock.results[0].value.send;

    await publishToDlt({
      message: { key: Buffer.from('order-1'), value: Buffer.from('{}') },
      error: new Error('Invalid event'),
    });

    expect(producerSend).toHaveBeenCalledWith(expect.objectContaining({
      topic: 'orders.events.dlt',
      acks: -1,
    }));
  });

  test('retries transaction conflicts instead of sending valid events to the DLT', async () => {
    const { handleMessage } = require('../../../workers/analyticsConsumer');
    const { Kafka } = require('kafkajs');
    const kafka = Kafka.mock.results[0].value;
    consumer = kafka.consumer.mock.results[0].value;
    dltProducerSend = kafka.producer.mock.results[0].value.send;
    const conflict = Object.assign(new Error('TransactionConflict'), {
      name: 'TransactionCanceledException',
      CancellationReasons: [{ Code: 'TransactionConflict' }],
    });
    dynamo.send
      .mockRejectedValueOnce(conflict)
      .mockResolvedValueOnce({});
    const event = {
      eventId: 'event-1',
      eventType: 'ORDER_CREATED',
      source: 'load-test',
      occurredAt: '2026-11-17T19:00:00.000Z',
      data: { orderId: 'order-1', status: 'PENDING', total: 1 },
    };

    await handleMessage({
      topic: 'orders.events',
      partition: 0,
      message: { value: Buffer.from(JSON.stringify(event)), offset: '4' },
    });

    expect(dynamo.send).toHaveBeenCalledTimes(2);
    expect(dltProducerSend).not.toHaveBeenCalled();
    expect(consumer.commitOffsets).toHaveBeenCalledWith([{
      topic: 'orders.events',
      partition: 0,
      offset: '5',
    }]);
  });
});
