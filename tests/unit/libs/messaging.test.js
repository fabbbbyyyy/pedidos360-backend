const { parseMessage } = require('../../../src/functions/notify/consumers/shared/createHandler');

describe('notification message contract', () => {
  test('accepts ORDER_CREATED payloads with orderId', () => {
    const message = parseMessage({
      body: JSON.stringify({
        eventId: 'ORDER_CREATED#order-1',
        eventType: 'ORDER_CREATED',
        data: { orderId: 'order-1' },
      }),
    });

    expect(message.data.orderId).toBe('order-1');
  });

  test('rejects messages without an order identifier', () => {
    expect(() => parseMessage({
      body: JSON.stringify({
        eventId: 'ORDER_CREATED#missing',
        eventType: 'ORDER_CREATED',
        data: {},
      }),
    })).toThrow('Mensaje sin eventId, eventType o data.orderId');
  });

  test('rejects payloads that only contain id', () => {
    expect(() => parseMessage({
      body: JSON.stringify({
        eventId: 'ORDER_CREATED#order-1',
        eventType: 'ORDER_CREATED',
        data: { id: 'order-1' },
      }),
    })).toThrow('Mensaje sin eventId, eventType o data.orderId');
  });
});
