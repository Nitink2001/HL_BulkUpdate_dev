
//HISTORY: had to move to V3javascript
// const { s3 } = require("../config/awsConfig")
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const csvParser = require("csv-parser");
const { Readable } = require("stream");

const s3Client = new S3Client({
    region: "eu-west-2",
  });
/**
 * 
 * @param {string} fileUrl
 * @returns {Promise<Array>}
 */
const readFileDataFromS3 = async (fileUrl) => {

  const { Bucket, Key } = parseS3Url(fileUrl);
    console.log("Bucket", Bucket)
    console.log("Key", Key)
    // console.log("s3", s3.config)
  try {
    const command = new GetObjectCommand({ Bucket, Key });
    const response = await s3Client.send(command);
    
    const fileStream = response.Body;
    const chunks = [];
    for await (const chunk of response.Body) {
        chunks.push(chunk);
    }
    const data = Buffer.concat(chunks).toString("utf-8");

    if (Key.endsWith(".csv")) {
      return parseCsv(data);
    } else if (Key.endsWith(".json")) {
      return parseJson(fileStream);
    } else {
      throw new Error("Unsupported file format. Only CSV and JSON are supported.");
    }
  } catch (error) {
    console.error("Error reading file from S3:", error);
    throw new Error(`Failed to read or parse file from S3: ${error.message}`);
  }
};

/**
 * 
 * @param {Buffer} buffer 
 * @returns {Promise<Array>}
 */
const parseCsv = (buffer) => {
  return new Promise((resolve, reject) => {
    const records = [];
    const readableStream = Readable.from(buffer.toString("utf-8"));

    readableStream
      .pipe(csvParser())
      .on("data", (row) => records.push(row))
      .on("end", () => resolve(records))
      .on("error", (err) => reject(err));
  });
};

/**
 * 
 * @param {Buffer} buffer 
 * @returns {Array} 
 */
const parseJson = (buffer) => {
  try {
    return JSON.parse(buffer.toString("utf-8"));
  } catch (err) {
    throw new Error("Failed to parse JSON file: " + err.message);
  }
};

/**
 * 
 * @param {string} s3Url 
 * @returns {Object}
 */
const parseS3Url = (s3Url) => {
  const match = s3Url.match(/^s3:\/\/([^/]+)\/(.+)$/);
  if (!match) {
    throw new Error("Invalid S3 URL format. Must be in the format s3://bucket-name/key.");
  }
  return { Bucket: match[1], Key: match[2] };
};

module.exports = { readFileDataFromS3 };
