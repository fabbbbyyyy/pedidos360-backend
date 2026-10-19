const { createConsumerHandler } = require('../shared/createHandler');

const handler = createConsumerHandler({
  consumerName: 'status',
  dlqUrl: process.env.NOTIFY_STATUS_DLQ_URL,
  processMessage: async (message) => {
    console.log(JSON.stringify({
      level: 'INFO',
      message: 'Evento de estado recibido',
      eventId: message.eventId,
      orderId: message.data.orderId,
      status: message.data.status,
    }));
  },
});

module.exports = { handler };
