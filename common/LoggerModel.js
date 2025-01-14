exports.Logger = async function (LogLevel, LogMessage, LogDetails) {
    // require('log-timestamp');
    if (!LogDetails) {
        console.log(LogLevel, LogMessage, '');
    } else {
        console.log(LogLevel, LogMessage, LogDetails);
    }
};

exports.errorExecutionTime = function (entryAPITime, errorExitTime, APIName) {
    this.Logger('ERROR', `[${APIName}] API error exit time `, errorExitTime);
    let totalErrorExecutionTime = errorExitTime.getTime() - entryAPITime.getTime();
    this.Logger('ERROR', `[${APIName}] API error total execution time: ${totalErrorExecutionTime}`, 'ms');
};

exports.successExecutionTime = function (entryAPITime, successExitTime, APIName) {
    this.Logger('INFO', `[${APIName}] API success exit time `, successExitTime);
    let totalSuccessExecutionTime = successExitTime.getTime() - entryAPITime.getTime();
    this.Logger('INFO', `[${APIName}] API success total execution time: ${totalSuccessExecutionTime}`, 'ms');
};
