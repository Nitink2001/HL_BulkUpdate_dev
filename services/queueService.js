const { sqs } = require("../config/awsConfig");
const { updateActionStatus } = require("./dynamoService");
const { readFileDataFromS3 } = require("./s3Service");
const BATCH_SIZE = 100;

exports.enqueueBatches = async (actionRecord) => {
  const { actionId, fileUrl, entityType, fieldsToUpdate } = actionRecord;

  console.log("fileUrl", fileUrl);

  const allData = await readFileDataFromS3(fileUrl);
  console.log("allData", allData);

  for (let i = 0; i < allData.length; i += BATCH_SIZE) {
    const batch = allData.slice(i, i + BATCH_SIZE);

    const messageBody = {
      actionId,
      records: batch,
      entityType,
      fieldsToUpdate, 
    };

    console.log("process.env.SQS_QUEUE_URL", process.env.SQS_QUEUE_URL);
    const params = {
      QueueUrl: process.env.SQS_QUEUE_URL,
      MessageBody: JSON.stringify(messageBody),
    };

    await sqs.sendMessage(params).promise();
  }

  await updateActionStatus(actionId, "IN_PROGRESS");
};
