const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');

const sqs = new SQSClient({});

class NonRecoverableError extends Error {}

const parseMessage = (record) => {
  let message;
  try {
    message = JSON.parse(record.body);
  } catch (err) {
    throw new NonRecoverableError(`Mensaje JSON inválido: ${err.message}`);
  }

  if (!message.eventId || !message.eventType || !message.data?.orderId) {
    throw new NonRecoverableError('Mensaje sin eventId, eventType o data.orderId');
  }

  return message;
};

const createConsumerHandler = ({ consumerName, dlqUrl, processMessage }) => async (event) => {
  const batchItemFailures = [];

  for (const record of event.Records || []) {
    try {
      const message = parseMessage(record);
      await processMessage(message);
    } catch (err) {
      if (err instanceof NonRecoverableError) {
        console.error(JSON.stringify({
          level: 'ERROR',
          message: 'Mensaje no recuperable enviado manualmente a DLQ',
          consumerName,
          messageId: record.messageId,
          error: err.message,
        }));
        await sqs.send(new SendMessageCommand({
          QueueUrl: dlqUrl,
          MessageBody: record.body,
        }));
      } else {
        console.warn(JSON.stringify({
          level: 'WARN',
          message: 'Error recuperable; se solicita reintento',
          consumerName,
          messageId: record.messageId,
          error: err.message,
        }));
        batchItemFailures.push({ itemIdentifier: record.messageId });
      }
    }
  }

  return { batchItemFailures };
};

module.exports = { NonRecoverableError, createConsumerHandler, parseMessage };
