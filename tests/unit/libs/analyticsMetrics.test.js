const { buildAnalyticsTransaction, minuteBucket } = require('../../../workers/analyticsMetrics');

describe('analytics metrics', () => {
  test('uses the event timestamp as a minute bucket', () => {
    expect(minuteBucket('2026-11-16T19:35:42.000Z')).toBe('2026-11-16T19:35');
  });

  test('builds idempotent order-created metric updates', () => {
    const items = buildAnalyticsTransaction({
      tableName: 'analytics',
      event: {
        eventId: 'ORDER_CREATED#order-1',
        eventType: 'ORDER_CREATED',
        occurredAt: '2026-11-16T19:35:42.000Z',
        data: { orderId: 'order-1', status: 'PENDING', total: 1250 },
      },
    });

    expect(items).toHaveLength(3);
    expect(items[0].Update.Key).toEqual({ pk: 'METRIC#MINUTE', sk: '2026-11-16T19:35' });
    expect(items[1].Update.Key).toEqual({ pk: 'METRIC#STATUS', sk: 'PENDING' });
    expect(items[2].Put.ConditionExpression).toBe('attribute_not_exists(pk)');
  });

  test('moves status counts for status changes', () => {
    const items = buildAnalyticsTransaction({
      tableName: 'analytics',
      event: {
        eventId: 'ORDER_STATUS_CHANGED#order-1#2',
        eventType: 'ORDER_STATUS_CHANGED',
        occurredAt: '2026-11-16T19:36:42.000Z',
        data: {
          orderId: 'order-1',
          previousStatus: 'PENDING',
          status: 'CONFIRMED',
        },
      },
    });

    expect(items).toHaveLength(4);
    expect(items[1].Update.Key).toEqual({ pk: 'METRIC#STATUS', sk: 'PENDING' });
    expect(items[2].Update.Key).toEqual({ pk: 'METRIC#STATUS', sk: 'CONFIRMED' });
  });
});
