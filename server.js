const serverless = require('serverless-http');
const app = require('./app');
const LoggerModel = require('./common/LoggerModel');
const port = 3000;

if (!module.parent) {
    app.listen(port, () => {
        LoggerModel.Logger('INFO', `Bulk Upload listening at http://localhost:${port}`);
    });
}

module.exports.handler = serverless(app);