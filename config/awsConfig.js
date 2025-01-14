
const AWS = require("aws-sdk");


AWS.config.update({
  region: "eu-west-2", 
});


const s3 = new AWS.S3();
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const sqs = new AWS.SQS();
const rds = new AWS.RDSDataService();
const eventBridge = new AWS.EventBridge();

module.exports = { s3, dynamoDB, sqs, rds, eventBridge };
