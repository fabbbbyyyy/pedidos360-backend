const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { createEvent } = require('./eventContract');

const sns = new SNSClient({});
const NOTIFY_TOPIC_ARN = process.env.NOTIFY_TOPIC_ARN;

const publishNotification = async ({ eventType, data, eventId, occurredAt, source }) => {
  if (!NOTIFY_TOPIC_ARN) {
    throw new Error('NOTIFY_TOPIC_ARN no está configurado');
  }

  const event = createEvent({ eventId, eventType, data, occurredAt, source });

  await sns.send(new PublishCommand({
    TopicArn: NOTIFY_TOPIC_ARN,
    Message: JSON.stringify(event),
  }));
};

module.exports = { publishNotification };
