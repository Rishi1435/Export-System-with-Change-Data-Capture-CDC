const express = require('express');
const { logger } = require('./logger');
const healthRouter = require('./routes/health');
const exportsRouter = require('./routes/exports');

const app = express();

app.use(express.json());

// Request logging middleware
app.use((req, res, next) => {
    logger.info({ req: { method: req.method, url: req.url } }, 'Incoming request');
    next();
});

app.use('/health', healthRouter);
app.use('/exports', exportsRouter);

// Error handling middleware
app.use((err, req, res, next) => {
    logger.error(err, 'Unhandled error');
    res.status(500).json({ error: 'Internal Server Error' });
});

module.exports = app;
