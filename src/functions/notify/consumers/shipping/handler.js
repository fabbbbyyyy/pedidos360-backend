const { createConsumerHandler } = require('../shared/createHandler');

const handler = createConsumerHandler({
  consumerName: 'shipping',
  dlqUrl: process.env.NOTIFY_SHIPPING_DLQ_URL,
  processMessage: async (message) => {
    console.log(JSON.stringify({
      level: 'INFO',
      message: 'Evento de shipping recibido',
      eventId: message.eventId,
      orderId: message.data.orderId,
    }));
  },
});

module.exports = { handler };
