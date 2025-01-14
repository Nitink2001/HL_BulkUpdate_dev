const CloudwatchLogger = require('./LoggerModel');
exports.errorResponse = (APIName, RefNo, error, entryTime, exitTime) => {
    if (typeof error === 'object' && error !== null && 'status' in error) {
        delete error['status'];
    }
    CloudwatchLogger.errorExecutionTime(entryTime, exitTime, APIName);
    return {
        RefNo: RefNo,
        message: 'ERROR',
        error
    };
};

exports.successResponse = (APIName, RefNo, data, entryTime, exitTime) => {
    if (data && data['status']) {
        delete data['status'];
    }
    CloudwatchLogger.successExecutionTime(entryTime, exitTime, APIName);
    return {
        RefNo: RefNo,
        message: 'SUCCESS',
        data
    };
};