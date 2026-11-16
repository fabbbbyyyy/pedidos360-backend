const { getRange, minuteBucket } = require('../../../src/functions/report/handler');

describe('analytics report range', () => {
  test('normalizes ISO timestamps to minute buckets', () => {
    expect(minuteBucket('2026-11-16T19:35:42.000Z')).toBe('2026-11-16T19:35');
  });

  test('rejects an inverted range', () => {
    expect(() => getRange({
      queryStringParameters: {
        from: '2026-11-16T20:00:00.000Z',
        to: '2026-11-16T19:00:00.000Z',
      },
    })).toThrow('from debe ser anterior o igual a to');
  });
});
