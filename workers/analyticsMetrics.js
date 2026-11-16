const minuteBucket = (occurredAt) => {
  const date = new Date(occurredAt);
  if (Number.isNaN(date.getTime())) {
    throw new Error('occurredAt no es una fecha válida');
  }
  return date.toISOString().slice(0, 16);
};

const metricKey = (type, value) => {
  if (type === 'EVENT') {
    return { pk: `EVENT#${value}`, sk: `EVENT#${value}` };
  }
  return { pk: `METRIC#${type}`, sk: value };
};

const buildAnalyticsTransaction = ({ event, tableName }) => {
  const minute = minuteBucket(event.occurredAt);
  const minuteValues = {
    ':entityType': 'MINUTE_METRIC',
    ':bucket': minute,
    ':one': 1,
  };
  const minuteAdds = ['eventsProcessed :one'];

  if (event.eventType === 'ORDER_CREATED') {
    minuteAdds.push('ordersCreated :one', 'salesTotal :sales');
    minuteValues[':sales'] = Number(event.data.total || 0);
  }

  if (event.eventType === 'ORDER_STATUS_CHANGED') {
    minuteAdds.push('statusChanges :one');
  }

  const transactItems = [{
    Update: {
      TableName: tableName,
      Key: metricKey('MINUTE', minute),
      UpdateExpression: `SET entityType = :entityType, bucketStart = :bucket ADD ${minuteAdds.join(', ')}`,
      ExpressionAttributeValues: minuteValues,
    },
  }];

  if (event.eventType === 'ORDER_CREATED') {
    transactItems.push({
      Update: {
        TableName: tableName,
        Key: metricKey('STATUS', event.data.status || 'PENDING'),
        UpdateExpression: 'SET entityType = :entityType ADD ordersCount :one',
        ExpressionAttributeValues: {
          ':entityType': 'STATUS_METRIC',
          ':one': 1,
        },
      },
    });
  }

  if (event.eventType === 'ORDER_STATUS_CHANGED') {
    const previousStatus = event.data.previousStatus;
    const currentStatus = event.data.status;
    if (previousStatus !== currentStatus) {
      transactItems.push(
        {
          Update: {
            TableName: tableName,
            Key: metricKey('STATUS', previousStatus),
            UpdateExpression: 'SET entityType = :entityType ADD ordersCount :one',
            ExpressionAttributeValues: {
              ':entityType': 'STATUS_METRIC',
              ':one': -1,
            },
          },
        },
        {
          Update: {
            TableName: tableName,
            Key: metricKey('STATUS', currentStatus),
            UpdateExpression: 'SET entityType = :entityType ADD ordersCount :one',
            ExpressionAttributeValues: {
              ':entityType': 'STATUS_METRIC',
              ':one': 1,
            },
          },
        },
      );
    }
  }

  transactItems.push({
    Put: {
      TableName: tableName,
      Item: {
        ...metricKey('EVENT', event.eventId),
        entityType: 'PROCESSED_EVENT',
        eventId: event.eventId,
        processedAt: new Date().toISOString(),
      },
      ConditionExpression: 'attribute_not_exists(pk)',
    },
  });

  return transactItems;
};

module.exports = { buildAnalyticsTransaction, minuteBucket, metricKey };
