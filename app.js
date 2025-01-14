const express = require('express');
const app = express();
const cors = require('cors');
const bodyParser = require('body-parser');
require('dotenv').config();
const routes = require('./router/route');

app.use(bodyParser.json({ limit: '10mb' }));

app.use(cors());

routes(app);

module.exports = app;