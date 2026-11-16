const { createEvent, parseEvent } = require('../../../src/libs/messaging/eventContract');

describe('event contract', () => {
  test('creates a version-compatible order event envelope', () => {
    const event = createEvent({
      eventId: 'ORDER_CREATED#order-1',
      eventType: 'ORDER_CREATED',
      data: { orderId: 'order-1', total: 100 },
    });

    expect(event).toEqual(expect.objectContaining({
      eventId: 'ORDER_CREATED#order-1',
      eventType: 'ORDER_CREATED',
      source: 'pedidos360-orders',
      data: { orderId: 'order-1', total: 100 },
    }));
    expect(() => new Date(event.occurredAt).toISOString()).not.toThrow();
  });

  test('rejects events without an order identifier', () => {
    expect(() => createEvent({
      eventId: 'ORDER_CREATED#missing',
      eventType: 'ORDER_CREATED',
      data: {},
    })).toThrow();
  });

  test('parses serialized events for bridge and Kafka consumers', () => {
    const event = parseEvent(JSON.stringify({
      eventId: 'ORDER_STATUS_CHANGED#order-1#2',
      eventType: 'ORDER_STATUS_CHANGED',
      source: 'pedidos360-orders',
      occurredAt: '2026-11-16T18:00:00.000Z',
      data: { orderId: 'order-1', version: 2 },
    }));

    expect(event.data.orderId).toBe('order-1');
  });
});
