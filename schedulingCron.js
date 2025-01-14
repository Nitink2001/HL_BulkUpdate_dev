const { listActionRecords, updateActionStatus } = require("../services/dynamoService");
const { enqueueBatches } = require("../services/queueService");

exports.handler = async () => {
  const now = new Date().toISOString();
  const actions = await listActionRecords(); 

  for (const action of actions) {
    if (action.status === "SCHEDULED" && action.scheduledTime <= now) {
      await updateActionStatus(action.actionId, "QUEUED");
      await enqueueBatches(action);
    }
  }

  return { message: "Schedule check complete" };
};
