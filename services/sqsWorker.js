

const { rds } = require('../config/awsConfig')
const { updateActionStats, checkDuplicate, markEmailAsProcessed, updateActionStatus } = require("./dynamoService");



const MAX_RETRIES = 5;
const RETRY_DELAY = 100;
const DYNAMODB_TABLE = process.env.DYNAMODB_TABLE || "BulkActions";


const DB_CLUSTER_ARN = process.env.DB_CLUSTER_ARN;
const DB_SECRET_ARN = process.env.DB_SECRET_ARN;
const DATABASE_NAME = process.env.DATABASE_NAME;

/**
 * @param {string} actionId 
 * @param {string} email 
 */
const markEmailAsProcessed = async (actionId, email) => {
    const params = {
      TableName: DYNAMODB_TABLE,
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

const logAction = async (actionId, eventType, details) => {
  await insertActionLog(actionId, {
    logId: `${eventType}#${new Date().toISOString()}`,
    timestamp: new Date().toISOString(),
    event: eventType,
    details,
  });
};

const insertActionLog = async (actionId, log) => {
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
 * @returns {Promise<boolean>} 
 */
const checkDuplicate = async (actionId, email) => {
  const params = {
    TableName: DYNAMODB_TABLE,
    Key: {
      PK: `ACTION#${actionId}`,
      SK: `PROCESSED_EMAILS`,
    },
    ProjectionExpression: "emails",
  };

  try {
    const result = await dynamoDB.get(params).promise();
    const processedEmailsSet = result.Item?.emails;

    // Convert DynamoDB Set to an array
    const processedEmails = processedEmailsSet ? processedEmailsSet.values : [];
    return processedEmails.includes(email);
  } catch (error) {
    console.error("Error checking duplicate:", error);
    throw error;
  }
};

/**
 * @param {string} actionId 
 * @param {number} successDelta 
 * @param {number} failureDelta 
 * @param {number} skippedDelta 
 */
const updateActionStats = async (actionId, successDelta, failureDelta, skippedDelta) => {
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


export const handler = async (event) => {
  console.log("Events:", event);

  for (const record of event.Records) {
    const body = JSON.parse(record.body);
    const { actionId, records, entityType, fieldsToUpdate } = body;

    let successCount = 0;
    let failureCount = 0;
    let skippedCount = 0;

    try {
      for (const entity of records) {
        try {
          const isDuplicate = await checkDuplicate(actionId, entity.email);
          if (isDuplicate) {
            console.log(`Skipping duplicate email: ${entity.email}`);
            skippedCount++;
            await logAction(actionId, "SKIP", {
              entity,
              reason: "Duplicate email",
            });
            continue;
          }

          await retryWithBackoff(() => updateContactInRDS(entity, entityType, fieldsToUpdate));

          await retryWithBackoff(() =>
            markEmailAsProcessed(actionId, entity.email)
          );

          successCount++;
          await logAction(actionId, "SUCCESS", {
            entity,
            message: "Entity processed successfully",
          });
        } catch (error) {
          console.error("Failed to process entity:", entity, error);
          failureCount++;
          await logAction(actionId, "FAILURE", {
            entity,
            error: error.message,
          });
        }
      }

      await retryWithBackoff(() =>
        updateActionStats(actionId, successCount, failureCount, skippedCount)
      );

      if (successCount > 0 && failureCount === 0) {
        await retryWithBackoff(() => updateActionStatus(actionId, "COMPLETED"));
      } else if (successCount > 0 && failureCount > 0) {
        await retryWithBackoff(() => updateActionStatus(actionId, "PARTIALLY_COMPLETED"));
      } else if (successCount === 0 && failureCount > 0) {
        await retryWithBackoff(() => updateActionStatus(actionId, "FAILED"));
      }
    } catch (err) {
      console.error("Critical error during batch processing:", err);
      await retryWithBackoff(() => updateActionStatus(actionId, "FAILED"));
      await logAction(actionId, "STATUS_UPDATE", {
        status: "FAILED",
        error: err.message,
      });
    }
  }

  return {
    statusCode: 200,
    body: JSON.stringify({ message: "Batch processed successfully" }),
  };
};


const updateActionStatus = async (actionId, status) => {
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
 * Updates a contact in the Aurora database using the Data API.
 * @param {Object} entity - The contact entity (e.g., { name, email, age }).
 */
const ENTITY_VALID_FIELDS = {
  contacts: ["name", "age", "email", "phone"],
  companies: ["name", "industry", "size", "location"],
  tasks: ["title", "dueDate", "priority", "status"],
  leads: ["name", "source", "stage", "email"],
};

const updateContactInRDS = async (entity, entityType, fieldsToUpdate) => {
  console.log("entityType", entityType)
  console.log("entity", entity)
  console.log("fieldsToUpdate", fieldsToUpdate)
  console.log("ENTITY_VALID_FIELDS.entityType", ENTITY_VALID_FIELDS[entityType])
  const validFieldsForEntity = ENTITY_VALID_FIELDS[entityType];

  if (!validFieldsForEntity) {
    throw new Error(`Invalid entityType: ${entityType}.`);
  }

  const validFields = fieldsToUpdate.filter((field) => validFieldsForEntity.includes(field));

  if (validFields.length === 0) {
    throw new Error(`No valid fields to update for entity type: ${entityType}.`);
  }

  const updateFields = validFields.map(
    (field) => `${field} = VALUES(${field})`
  ).join(", ");

  const insertFields = validFields.join(", ");
  const insertPlaceholders = validFields.map(
    (field) => `:${field}`
  ).join(", ");

  const sql = `
    INSERT INTO ${entityType} (email, ${insertFields})
    VALUES (:email, ${insertPlaceholders})
    ON DUPLICATE KEY UPDATE ${updateFields};
  `;

  const parameters = validFields.map((field) => ({
    name: field,
    value: { stringValue: entity[field] },
  }));
  parameters.push({ name: "email", value: { stringValue: entity.email } });

  const params = {
    resourceArn: DB_CLUSTER_ARN,
    secretArn: DB_SECRET_ARN,
    database: DATABASE_NAME,
    sql,
    parameters,
  };

  console.log("RDS Query Execution:", sql, parameters);
  try {
    const result = await rds.executeStatement(params).promise();
    console.log("Update Result:", result);
  } catch (error) {
    console.error("Error updating entity:", error);
    throw error;
  }
};


/**
 * Retry a function with exponential backoff.
 * @param {Function} fn - The function to execute.
 * @param {number} retries - Remaining retry attempts (default: MAX_RETRIES).
 * @param {number} delay - Delay between retries in milliseconds (default: RETRY_DELAY).
 * @returns {Promise<void>}
 */
const retryWithBackoff = async (fn, retries = MAX_RETRIES, delay = RETRY_DELAY) => {
  try {
    await fn();
  } catch (error) {
    if (retries <= 0) {
      console.error("Max retries reached. Failing operation:", error);
      throw error;
    }

    console.warn(`Retrying operation... Attempts left: ${retries}`);
    await new Promise((resolve) => setTimeout(resolve, delay));
    return retryWithBackoff(fn, retries - 1, delay * 2);
  }
};
