const { dynamoDB } = require("../config/awsConfig");

const TABLE_NAME = process.env.DYNAMODB_TABLE || "BulkActions";

/**
 * @param {string} accountId 
 * @param {number} maxRate 
 * @returns {Promise<boolean>} 
 */
exports.checkRateLimit = async (accountId, maxRate) => {
  
  const now = new Date();
  const minuteBucket = `${now.getUTCFullYear()}-${now.getUTCMonth() + 1}-${now.getUTCDate()}-${now.getUTCHours()}-${now.getUTCMinutes()}`;

  const pk = `ACCOUNT#${accountId}`;
  const sk = `RATE_LIMIT#${minuteBucket}`;

  console.log("pk", pk)
  console.log("sk", sk)
  let currentUsage = 0;
  const getParams = {
    TableName: TABLE_NAME,
    Key: { PK: pk, SK: sk },
  };
  const result = await dynamoDB.get(getParams).promise();
  if (result.Item) {
    currentUsage = result.Item.usageDetail;
  }

  if (currentUsage >= maxRate) {
    return false; 
  }

  const updateParams = {
    TableName: TABLE_NAME,
    Key: { PK: pk, SK: sk },
    UpdateExpression: "ADD usageDetail :inc",
    ExpressionAttributeValues: {
      ":inc": 1,
    },
    ReturnValues: "UPDATED_NEW",
  };
  await dynamoDB.update(updateParams).promise();

  return true;
};
