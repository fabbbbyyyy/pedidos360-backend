const { createConsumerHandler } = require('../shared/createHandler');

const handler = createConsumerHandler({
  consumerName: 'email',
  dlqUrl: process.env.NOTIFY_EMAIL_DLQ_URL,
  processMessage: async (message) => {
    console.log(JSON.stringify({
      level: 'INFO',
      message: 'Evento de email recibido',
      eventId: message.eventId,
      orderId: message.data.orderId,
    }));
  },
});

module.exports = { handler };
