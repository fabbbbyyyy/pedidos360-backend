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
    ({ recoverExpiredLeases } = require('../../../workers/outboxPublisher'));
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
});
