const cors = require('cors');
const {
    getAllBulkActions,
    createBulkAction,
    getBulkActionById,
    getBulkActionStats,
    getBulkActionLogs
  } = require("../controllers/bulkActionController");


  module.exports = function (app) {

    app.route("/api/v1/").get(getAllBulkActions, cors())
    app.route("/api/v1/").post(createBulkAction, cors())
    app.route("/api/v1/:actionId").get(getBulkActionById, cors())
    app.route("/api/v1/:actionId/stats").get(getBulkActionStats, cors()) 
    app.route("/api/v1/:actionId/logs").get(getBulkActionLogs, cors())

  }