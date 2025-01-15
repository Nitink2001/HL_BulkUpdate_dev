const { createActionRecord, getActionRecord, listActionRecords, updateActionStatus, getActionLogs } = require("../services/dynamoService");
const { enqueueBatches } = require("../services/queueService");
const { checkRateLimit } = require("../utils/rateLimiter");
const { eventBridge } = require("../config/awsConfig")
const LoggerModel = require('../common/LoggerModel');
const crypto = require("crypto");

const EVENT_BUS_NAME = "default"; 
const LAMBDA_FUNCTION_ARN = "arn:aws:lambda:eu-west-2:159488909220:function:scheduledBulkUpload"; 

exports.getAllBulkActions = async (req, res, next) => {
  try {
    const actions = await listActionRecords();
    res.json(actions);
  } catch (err) {
    next(err);
  }
};

const scheduleActionWithEventBridge = async (actionRecord) => {
    const { actionId, scheduledTime } = actionRecord;

    const params = {
        Entries: [
            {
                EventBusName: EVENT_BUS_NAME,
                Source: "bulk-action-scheduler",
                DetailType: "BulkActionScheduled",
                Time: new Date(scheduledTime), 
                Detail: JSON.stringify(actionRecord),
                Resources: [LAMBDA_FUNCTION_ARN],
            },
        ],
    };

    try {
        const result = await eventBridge.putEvents(params).promise();
        LoggerModel.Logger("INFO", `Scheduled action ${actionId} successfully: ${JSON.stringify(result)}`);
    } catch (error) {
        LoggerModel.Logger("ERROR", `Failed to schedule action ${actionId}: ${error.message}`);
        throw new Error(`Failed to schedule action: ${error.message}`);
    }
};

exports.createBulkAction = async (req, res, next) => {

    const entryAPITime = new Date();
    const APIName = 'Create Bulk Action';
    LoggerModel.Logger('INFO', `[${APIName}] API Entry Time is ${entryAPITime}`);

    try {
        const {
        accountId,
        fileUrl,
        entityType,
        actionType,
        fieldsToUpdate,
        scheduledTime
        } = req.body;

        if (!accountId || !fileUrl || !actionType || !fieldsToUpdate || !entityType) {
            return res.status(400).json({ message: "Missing required parameters" });
        }

        const isAllowed = await checkRateLimit(accountId, 10000); 
        if (!isAllowed) {
            return res.status(429).json({ message: "Rate limit exceeded" });
        }

        const actionId = crypto.randomBytes(8).toString("hex");

        const actionRecord = {
        actionId,
        accountId,
        fileUrl,
        entityType,
        actionType,
        fieldsToUpdate,
        status: scheduledTime ? "SCHEDULED" : "QUEUED",
        scheduledTime: scheduledTime || null,
        createdAt: new Date().toISOString(),
        };
        await createActionRecord(actionRecord);

        
        if (scheduledTime) {
        
            await scheduleActionWithEventBridge(actionRecord);
            LoggerModel.Logger("INFO", `[${APIName}] Scheduled action ${actionId} for ${scheduledTime}`);
        } else {
            await enqueueBatches(actionRecord);
            await updateActionStatus(actionId, "IN_PROGRESS");
            LoggerModel.Logger("INFO", `[${APIName}] action ${actionId} sent to Queue for Processing`);
        }

        res.status(201).json({
            message: "Bulk action created",
            actionId,
            status: scheduledTime ? "SCHEDULED" : "QUEUED",
        });
    } catch (err) {
        next(err);
    }
};



exports.getBulkActionById = async (req, res, next) => {
    const entryAPITime = new Date();
    const APIName = 'Get Bulk Action By id';
    LoggerModel.Logger('INFO', `[${APIName}] API Entry Time is ${entryAPITime}`);
    try {
        const { actionId } = req.params;
        const action = await getActionRecord(actionId, "METADATA");
        if (!action) {
            return res.status(404).json({ message: "Action not found" });
        }
        res.json(action);
    } catch (err) {
        next(err);
    }
};

exports.getBulkActionStats = async (req, res, next) => {
    const entryAPITime = new Date();
    const APIName = 'Insert Identity';
    LoggerModel.Logger('INFO', `[${APIName}] API Entry Time is ${entryAPITime}`);
    try {
        const { actionId } = req.params;
        const action = await getActionRecord(actionId, "STATISTICS");
        if (!action) {
            return res.status(404).json({ message: "Action not found" });
        }

        const stats = {
            totalProcessed: action.totalProcessed || 0,
            successCount: action.successCount || 0,
            failureCount: action.failureCount || 0,
            skippedCount: action.skippedCount || 0,
        };

        res.json(stats);
    } catch (err) {
        next(err);
    }
};

exports.getBulkActionLogs = async (req, res, next) => {
    const entryAPITime = new Date();
    const APIName = 'Get Bulk Action Logs';
    LoggerModel.Logger('INFO', `[${APIName}] API Entry Time is ${entryAPITime}`);
    try {
        const { actionId } = req.params;

        if (!actionId) {
            return res.status(400).json({ message: "Missing required parameter: actionId" });
        }

        const logs = await getActionLogs(actionId);

        if (!logs || logs.length === 0) {
            return res.status(404).json({ message: "No logs found for the specified actionId" });
        }

        res.status(200).json({ actionId, logs });
    } catch (err) {
        LoggerModel.Logger('ERROR', `[${APIName}] Error: ${err.message}`);
        next(err);
    }
};

