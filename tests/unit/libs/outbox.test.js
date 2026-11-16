const { createOutboxItem, outboxKey } = require('../../../src/libs/messaging/outbox');

describe('outbox events', () => {
  const event = {
    eventId: 'ORDER_CREATED#order-1',
    eventType: 'ORDER_CREATED',
    source: 'pedidos360-orders',
    occurredAt: '2026-11-16T18:00:00.000Z',
    data: { orderId: 'order-1' },
  };

  test('creates a pending item indexed by status and creation time', () => {
    expect(createOutboxItem(event)).toEqual(expect.objectContaining({
      ...outboxKey(event.eventId),
      entityType: 'OUTBOX_EVENT',
      status: 'PENDING',
      attempts: 0,
      gsi1pk: 'STATUS#PENDING',
      gsi1sk: `${event.occurredAt}#${event.eventId}`,
      eventId: event.eventId,
      data: event.data,
    }));
  });

  test('rejects events that cannot be published', () => {
    expect(() => createOutboxItem({
      ...event,
      data: {},
    })).toThrow();
  });
});
