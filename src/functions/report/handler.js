const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
const { dynamoDb } = require('../../libs/db/dynamoClient');
const { success } = require('../../libs/utils/response');
const { withErrorHandler } = require('../../libs/middlewares/errorHandler');
const { requireRoles } = require('../../libs/middlewares/requireRoles');
const PERMISSIONS = require('../../libs/constants/permissions');

const ANALYTICS_TABLE = process.env.ANALYTICS_TABLE;

const minuteBucket = (value) => {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    const error = new Error('Los parámetros from y to deben ser fechas ISO válidas');
    error.statusCode = 400;
    throw error;
  }
  return date.toISOString().slice(0, 16);
};

const getRange = (event) => {
  const now = new Date();
  const from = event.queryStringParameters?.from || new Date(now.getTime() - 60 * 60 * 1000).toISOString();
  const to = event.queryStringParameters?.to || now.toISOString();
  const fromBucket = minuteBucket(from);
  const toBucket = minuteBucket(to);

  if (fromBucket > toBucket) {
    const error = new Error('from debe ser anterior o igual a to');
    error.statusCode = 400;
    throw error;
  }

  return { fromBucket, toBucket };
};

const handler = async (event) => {
  if (!ANALYTICS_TABLE) {
    throw new Error('ANALYTICS_TABLE no está configurado');
  }

  const { fromBucket, toBucket } = getRange(event);
  const [minuteMetrics, statusMetrics] = await Promise.all([
    dynamoDb.send(new QueryCommand({
      TableName: ANALYTICS_TABLE,
      KeyConditionExpression: 'pk = :pk AND sk BETWEEN :from AND :to',
      ExpressionAttributeValues: {
        ':pk': 'METRIC#MINUTE',
        ':from': fromBucket,
        ':to': toBucket,
      },
      ScanIndexForward: true,
    })),
    dynamoDb.send(new QueryCommand({
      TableName: ANALYTICS_TABLE,
      KeyConditionExpression: 'pk = :pk',
      ExpressionAttributeValues: { ':pk': 'METRIC#STATUS' },
      ScanIndexForward: true,
    })),
  ]);

  return success({
    from: fromBucket,
    to: toBucket,
    minuteMetrics: minuteMetrics.Items || [],
    statusMetrics: statusMetrics.Items || [],
  });
};

module.exports = {
  handler: withErrorHandler(requireRoles(...PERMISSIONS.analytics.read)(handler)),
  getRange,
  minuteBucket,
};
