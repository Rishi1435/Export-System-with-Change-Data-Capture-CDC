const { Router } = require('express');
const { v4: uuidv4 } = require('uuid');
const { exportService } = require('../services/exportService');

const router = Router();

const requireConsumerId = (req, res, next) => {
    const consumerId = req.headers['x-consumer-id'];
    if (!consumerId || typeof consumerId !== 'string') {
        res.status(400).json({ error: 'Missing or invalid X-Consumer-ID header' });
        return;
    }
    next();
};

router.post('/full', requireConsumerId, async (req, res) => {
    const consumerId = req.headers['x-consumer-id'];
    const jobId = uuidv4();
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const outputFilename = `full_${consumerId}_${timestamp}.csv`;

    res.status(202).json({
        jobId,
        status: 'started',
        exportType: 'full',
        outputFilename
    });

    // Start background processing
    exportService.runFullExport(jobId, consumerId, outputFilename).catch(err => {
        // Already logged in service
    });
});

router.post('/incremental', requireConsumerId, async (req, res) => {
    const consumerId = req.headers['x-consumer-id'];
    const jobId = uuidv4();
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const outputFilename = `incremental_${consumerId}_${timestamp}.csv`;

    res.status(202).json({
        jobId,
        status: 'started',
        exportType: 'incremental',
        outputFilename
    });

    exportService.runIncrementalExport(jobId, consumerId, outputFilename).catch(err => { });
});

router.post('/delta', requireConsumerId, async (req, res) => {
    const consumerId = req.headers['x-consumer-id'];
    const jobId = uuidv4();
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const outputFilename = `delta_${consumerId}_${timestamp}.csv`;

    res.status(202).json({
        jobId,
        status: 'started',
        exportType: 'delta',
        outputFilename
    });

    exportService.runDeltaExport(jobId, consumerId, outputFilename).catch(err => { });
});

router.get('/watermark', requireConsumerId, async (req, res) => {
    const consumerId = req.headers['x-consumer-id'];

    try {
        const watermark = await exportService.getWatermark(consumerId);
        if (!watermark) {
            res.status(404).json({ error: 'Watermark not found for consumer' });
            return;
        }

        res.status(200).json({
            consumerId,
            lastExportedAt: watermark.toISOString()
        });
    } catch (err) {
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

module.exports = router;
