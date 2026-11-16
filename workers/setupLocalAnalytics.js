const { CreateTableCommand, DynamoDBClient, DescribeTableCommand } = require('@aws-sdk/client-dynamodb');

const endpoint = process.env.DYNAMODB_ENDPOINT || 'http://localhost:8000';
const tableName = process.env.ANALYTICS_TABLE || 'pedidos360-analytics-local';
const client = new DynamoDBClient({
  region: process.env.AWS_REGION || 'us-east-1',
  endpoint,
  credentials: {
    accessKeyId: 'local',
    secretAccessKey: 'local',
  },
});

const run = async () => {
  try {
    await client.send(new DescribeTableCommand({ TableName: tableName }));
    console.log(`La tabla ${tableName} ya existe`);
    return;
  } catch (error) {
    if (error.name !== 'ResourceNotFoundException') {
      throw error;
    }
  }

  await client.send(new CreateTableCommand({
    TableName: tableName,
    BillingMode: 'PAY_PER_REQUEST',
    AttributeDefinitions: [
      { AttributeName: 'pk', AttributeType: 'S' },
      { AttributeName: 'sk', AttributeType: 'S' },
    ],
    KeySchema: [
      { AttributeName: 'pk', KeyType: 'HASH' },
      { AttributeName: 'sk', KeyType: 'RANGE' },
    ],
  }));
  console.log(`Tabla ${tableName} creada en ${endpoint}`);
};

run().catch((error) => {
  console.error('No se pudo preparar DynamoDB Local', error);
  process.exitCode = 1;
});
