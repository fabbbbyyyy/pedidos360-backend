jest.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: jest.fn(),
}));

jest.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: {
    from: jest.fn(() => ({ send: jest.fn() })),
  },
  QueryCommand: jest.fn((input) => ({ input })),
  UpdateCommand: jest.fn((input) => ({ input })),
}));

jest.mock('kafkajs', () => ({
  Kafka: jest.fn(() => ({
    producer: jest.fn(() => ({
      connect: jest.fn(),
      send: jest.fn(),
    })),
  })),
}));

describe('outbox publisher lease recovery', () => {
  let dynamo;
  let dynamoFrom;
  let producerSend;
  let buildKafkaMessage;
  let processBatch;
  let recoverExpiredLeases;

  beforeEach(() => {
    jest.resetModules();
    process.env.OUTBOX_TABLE = 'outbox';
    process.env.AWS_REGION = 'us-east-1';
    process.env.KAFKA_BROKERS = 'localhost:9092';
    process.env.KAFKA_TLS = 'false';

    ({ DynamoDBDocumentClient: { from: dynamoFrom } } = require('@aws-sdk/lib-dynamodb'));
    dynamo = { send: jest.fn() };
    dynamoFrom.mockReturnValue(dynamo);
    ({ buildKafkaMessage, processBatch, recoverExpiredLeases } = require('../../../workers/outboxPublisher'));
    const { Kafka } = require('kafkajs');
    producerSend = Kafka.mock.results[0].value.producer.mock.results[0].value.send;
  });

  afterEach(() => {
    delete process.env.OUTBOX_TABLE;
    delete process.env.AWS_REGION;
    delete process.env.KAFKA_BROKERS;
    delete process.env.KAFKA_TLS;
  });

  test('returns expired processing events to pending atomically', async () => {
    dynamo.send
      .mockResolvedValueOnce({
        Items: [{ pk: 'OUTBOX#1', sk: 'EVENT#1' }],
      })
      .mockResolvedValueOnce({});

    await recoverExpiredLeases();

    expect(dynamo.send).toHaveBeenCalledTimes(2);
    expect(dynamo.send.mock.calls[0][0].input).toEqual(expect.objectContaining({
      FilterExpression: 'leaseUntil <= :now',
      ExpressionAttributeValues: expect.objectContaining({
        ':status': 'STATUS#PROCESSING',
      }),
    }));
    expect(dynamo.send.mock.calls[1][0].input).toEqual(expect.objectContaining({
      ConditionExpression: '#status = :processing AND leaseUntil <= :now',
      UpdateExpression: expect.stringContaining('REMOVE leaseUntil'),
      ExpressionAttributeValues: expect.objectContaining({
        ':pendingIndex': 'STATUS#PENDING',
      }),
    }));
  });

  test('omits undefined headers so malformed envelopes can reach the analytics DLT', () => {
    const message = buildKafkaMessage({
      eventId: 'invalid-event',
      source: 'load-test',
      occurredAt: new Date().toISOString(),
      data: { orderId: 'order-1' },
    });

    expect(message.headers).toEqual({ eventId: 'invalid-event' });
    expect(JSON.parse(message.value)).not.toHaveProperty('eventType');
  });

  test('publishes outbox messages with acknowledgements from all replicas', async () => {
    const item = {
      pk: 'OUTBOX#1',
      sk: 'EVENT#1',
      eventId: 'event-1',
      eventType: 'order.created',
      source: 'orders',
      occurredAt: new Date().toISOString(),
      data: { orderId: 'order-1' },
    };
    dynamo.send
      .mockResolvedValueOnce({ Items: [] })
      .mockResolvedValueOnce({ Items: [item] })
      .mockResolvedValueOnce({ Attributes: item })
      .mockResolvedValueOnce({});

    await processBatch();

    expect(producerSend).toHaveBeenCalledWith(expect.objectContaining({
      topic: 'orders.events',
      acks: -1,
      messages: [expect.objectContaining({ key: 'order-1' })],
    }));
  });

  test('publishes a claimed batch in one Kafka request', async () => {
    const items = ['order-1', 'order-2'].map((orderId, index) => ({
      pk: `OUTBOX#${index + 1}`,
      sk: `EVENT#${index + 1}`,
      eventId: `event-${index + 1}`,
      eventType: 'ORDER_CREATED',
      source: 'load-test',
      occurredAt: '2026-11-17T19:00:00.000Z',
      data: { orderId },
    }));
    dynamo.send
      .mockResolvedValueOnce({ Items: [] })
      .mockResolvedValueOnce({ Items: items })
      .mockResolvedValueOnce({ Attributes: items[0] })
      .mockResolvedValueOnce({ Attributes: items[1] })
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({});

    await processBatch();

    expect(producerSend).toHaveBeenCalledTimes(1);
    expect(producerSend).toHaveBeenCalledWith(expect.objectContaining({
      topic: 'orders.events',
      acks: -1,
      messages: [
        expect.objectContaining({ key: 'order-1' }),
        expect.objectContaining({ key: 'order-2' }),
      ],
    }));
  });

  test('returns a claimed batch to pending when Kafka rejects the send', async () => {
    const item = {
      pk: 'OUTBOX#1',
      sk: 'EVENT#1',
      eventId: 'event-1',
      eventType: 'ORDER_CREATED',
      source: 'load-test',
      occurredAt: '2026-11-17T19:00:00.000Z',
      data: { orderId: 'order-1' },
    };
    dynamo.send
      .mockResolvedValueOnce({ Items: [] })
      .mockResolvedValueOnce({ Items: [item] })
      .mockResolvedValueOnce({ Attributes: item })
      .mockResolvedValueOnce({});
    producerSend.mockRejectedValueOnce(new Error('Kafka unavailable'));

    await expect(processBatch()).resolves.toBe(false);

    expect(dynamo.send.mock.calls[3][0].input).toEqual(expect.objectContaining({
      ConditionExpression: '#status = :processing',
      UpdateExpression: expect.stringContaining(':pendingIndex'),
    }));
  });
});
