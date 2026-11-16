const { z } = require('zod');

const eventSchema = z.object({
  eventId: z.string().min(1),
  eventType: z.string().min(1),
  source: z.string().min(1),
  occurredAt: z.string().datetime({ offset: true }),
  data: z.object({
    orderId: z.string().min(1),
  }).passthrough(),
}).strict();

const createEvent = ({ eventType, data, eventId, source = 'pedidos360-orders', occurredAt = new Date().toISOString() }) =>
  eventSchema.parse({
    eventId,
    eventType,
    source,
    occurredAt,
    data,
  });

const parseEvent = (message) => {
  const parsed = typeof message === 'string' ? JSON.parse(message) : message;
  return eventSchema.parse(parsed);
};

module.exports = { createEvent, parseEvent, eventSchema };
