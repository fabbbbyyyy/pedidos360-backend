const { createEvent } = require('./eventContract');

const outboxKey = (eventId) => ({
  pk: `EVENT#${eventId}`,
  sk: `EVENT#${eventId}`,
});

const createOutboxItem = (eventInput) => {
  const event = createEvent(eventInput);

  return {
    ...outboxKey(event.eventId),
    entityType: 'OUTBOX_EVENT',
    ...event,
    status: 'PENDING',
    attempts: 0,
    gsi1pk: 'STATUS#PENDING',
    gsi1sk: `${event.occurredAt}#${event.eventId}`,
  };
};

module.exports = { createOutboxItem, outboxKey };
