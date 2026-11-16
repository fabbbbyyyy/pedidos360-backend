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
  test('preserves the invalid payload and failure metadata', () => {
    process.env.ANALYTICS_TABLE = 'analytics';
    process.env.AWS_REGION = 'us-east-1';
    process.env.KAFKA_BROKERS = 'localhost:9092';
    process.env.KAFKA_TLS = 'false';

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
});
