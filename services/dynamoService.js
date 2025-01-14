const { dynamoDB } = require("../config/awsConfig");
const ActionTypes = require("../utils/actionTypes");

const TABLE_NAME = process.env.DYNAMODB_TABLE || "BulkActions";

/**
 * @param {Object} action 
 */
exports.createActionRecord = async (action) => {
    const timestamp = new Date().toISOString();
  
    const params = {
      TransactItems: [
        {
          Put: {
            TableName: TABLE_NAME,
            Item: {
              PK: `ACTION#${action.actionId}`,
              SK: "METADATA",
              ...action,
            },
          },
        },
        {
          Put: {
            TableName: TABLE_NAME,
            Item: {
              PK: "ACTION",
              SK: timestamp,
              actionId: action.actionId,
              name: action.name,
              createdAt: timestamp,
            },
          },
        },
      ],
    };
  
    try {
      await dynamoDB.transactWrite(params).promise();
      console.log("Action record created successfully:", action.actionId);
    } catch (error) {
      console.error("Error creating action record:", error);
      throw error;
    }
  };

/**

 * @param {string} actionId 
 * @param {string} status 
 */
exports.updateActionStatus = async (actionId, status) => {
  const params = {
    TableName: TABLE_NAME,
    Key: {
      PK: `ACTION#${actionId}`,
      SK: ActionTypes.METADATA,
    },
    UpdateExpression: "SET #s = :s",
    ExpressionAttributeNames: {
      "#s": "status",
    },
    ExpressionAttributeValues: {
      ":s": status,
    },
  };
  await dynamoDB.update(params).promise();
};

/**
 
 * @param {string} actionId 
 * @param {number} successDelta 
 * @param {number} failureDelta
 * @param {number} skippedDelta 
 */
exports.updateActionStats = async (actionId, successDelta, failureDelta, skippedDelta) => {
  const params = {
    TableName: TABLE_NAME,
    Key: {
      PK: `ACTION#${actionId}`,
      SK: ActionTypes.STATISTICS,
    },
    UpdateExpression: `
      ADD successCount :sc, failureCount :fc, skippedCount :sk, totalProcessed :tp
    `,
    ExpressionAttributeValues: {
      ":sc": successDelta,
      ":fc": failureDelta,
      ":sk": skippedDelta,
      ":tp": successDelta + failureDelta + skippedDelta,
    },
  };
  await dynamoDB.update(params).promise();
};

/**

 * @param {string} actionId 
 * @param {string} sk 
 */
exports.getActionRecord = async (actionId, sk) => {
  const params = {
    TableName: TABLE_NAME,
    Key: {
      PK: `ACTION#${actionId}`,
      SK: sk,
    },
  };
  const result = await dynamoDB.get(params).promise();
  return result.Item;
};

/**
 * @param {string} actionId 
 */
exports.getActionLogs = async (actionId) => {
  const params = {
    TableName: TABLE_NAME,
    KeyConditionExpression: "PK = :pk AND begins_with(SK, :prefix)",
    ExpressionAttributeValues: {
      ":pk": `ACTION#${actionId}`,
      ":prefix": `${ActionTypes.LOG}#`,
    },
  };
  const result = await dynamoDB.query(params).promise();
  return result.Items;
};

/**
 * @param {string} actionId 
 * @param {Object} log
 */
exports.insertActionLog = async (actionId, log) => {
  const params = {
    TableName: TABLE_NAME,
    Item: {
      PK: `ACTION#${actionId}`,
      SK: `${ActionTypes.LOG}#${log.logId}`,
      ...log,
    },
  };
  await dynamoDB.put(params).promise();
};

/**
 * @param {string} actionId 
 * @param {string} email 
 */
exports.markEmailAsProcessed = async (actionId, email) => {
    const params = {
      TableName: TABLE_NAME,
      Key: {
        PK: `ACTION#${actionId}`,
        SK: `PROCESSED_EMAILS`,
      },
      UpdateExpression: "ADD emails :email",
      ExpressionAttributeValues: {
        ":email": dynamoDB.createSet([email]),
      },
      ReturnValues: "UPDATED_NEW",
    };
  
    await dynamoDB.update(params).promise();
  };


/**

 * @param {string} actionId 
 * @param {string} email 
 * @returns {Promise<boolean>} 
 */
exports.checkDuplicate = async (actionId, email) => {
    const params = {
      TableName: DYNAMODB_TABLE,
      Key: {
        PK: `ACTION#${actionId}`,
        SK: `PROCESSED_EMAILS`,
      },
      ProjectionExpression: "emails",
    };
  
    const result = await dynamoDB.get(params).promise();
    const processedEmails = result.Item?.emails || [];
  
    return processedEmails.includes(email);
  };
/**
 * 
 * @returns {list} - List of actions
 */
exports.listActionRecords = async () => {

    const params = {
        TableName: TABLE_NAME,
        KeyConditionExpression: "PK = :pk",
        ExpressionAttributeValues: {
          ":pk": "ACTION",
        },
      };
    
      try {
        const result = await dynamoDB.query(params).promise();
        return result.Items;
      } catch (error) {
        console.error("Error fetching actions:", error);
        throw error;
      }
}
