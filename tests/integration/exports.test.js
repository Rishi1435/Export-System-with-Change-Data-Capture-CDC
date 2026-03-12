const request = require('supertest');
const app = require('../../src/app');
const { exportService } = require('../../src/services/exportService');

jest.mock('../../src/services/exportService');

describe('Exports Endpoints', () => {
    const consumerId = 'test-consumer';

    beforeEach(() => {
        jest.clearAllMocks();
    });

    describe('POST /exports/full', () => {
        it('should return 400 if X-Consumer-ID header is missing', async () => {
            const res = await request(app).post('/exports/full');
            expect(res.status).toBe(400);
        });

        it('should return 202 and start a full export job', async () => {
            exportService.runFullExport.mockResolvedValueOnce(undefined);

            const res = await request(app).post('/exports/full').set('X-Consumer-ID', consumerId);
            expect(res.status).toBe(202);
            expect(res.body).toHaveProperty('jobId');
            expect(res.body.exportType).toBe('full');
            expect(exportService.runFullExport).toHaveBeenCalledWith(
                res.body.jobId,
                consumerId,
                res.body.outputFilename
            );
        });
    });

    describe('POST /exports/incremental', () => {
        it('should return 202 and start an incremental export job', async () => {
            exportService.runIncrementalExport.mockResolvedValueOnce(undefined);

            const res = await request(app).post('/exports/incremental').set('X-Consumer-ID', consumerId);
            expect(res.status).toBe(202);
            expect(res.body).toHaveProperty('jobId');
            expect(res.body.exportType).toBe('incremental');
            expect(exportService.runIncrementalExport).toHaveBeenCalled();
        });
    });

    describe('POST /exports/delta', () => {
        it('should return 202 and start a delta export job', async () => {
            exportService.runDeltaExport.mockResolvedValueOnce(undefined);

            const res = await request(app).post('/exports/delta').set('X-Consumer-ID', consumerId);
            expect(res.status).toBe(202);
            expect(res.body).toHaveProperty('jobId');
            expect(res.body.exportType).toBe('delta');
            expect(exportService.runDeltaExport).toHaveBeenCalled();
        });
    });

    describe('GET /exports/watermark', () => {
        it('should return 404 if watermark not found', async () => {
            exportService.getWatermark.mockResolvedValueOnce(null);

            const res = await request(app).get('/exports/watermark').set('X-Consumer-ID', consumerId);
            expect(res.status).toBe(404);
        });

        it('should return 200 with the watermark', async () => {
            const date = new Date();
            exportService.getWatermark.mockResolvedValueOnce(date);

            const res = await request(app).get('/exports/watermark').set('X-Consumer-ID', consumerId);
            expect(res.status).toBe(200);
            expect(res.body.lastExportedAt).toBe(date.toISOString());
        });
    });
});
