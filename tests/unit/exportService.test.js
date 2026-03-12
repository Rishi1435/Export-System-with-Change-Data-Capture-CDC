const { exportService } = require('../../src/services/exportService');
const { pool } = require('../../src/db');

jest.mock('../../src/db', () => ({
    pool: {
        query: jest.fn(),
        connect: jest.fn(),
    }
}));

jest.mock('fs', () => ({
    createWriteStream: jest.fn().mockReturnValue({
        on: jest.fn((event, cb) => {
            if (event === 'finish') cb();
        }),
    }),
}));

jest.mock('csv-stringify', () => ({
    stringify: jest.fn().mockReturnValue({
        pipe: jest.fn(),
        write: jest.fn(),
        end: jest.fn(),
    }),
}));

describe('ExportService', () => {
    let mockClient;

    beforeEach(() => {
        jest.clearAllMocks();
        mockClient = {
            query: jest.fn(),
            release: jest.fn()
        };
    });

    describe('getWatermark', () => {
        it('should return null if no watermark found', async () => {
            pool.query.mockResolvedValueOnce({ rows: [] });
            const watermark = await exportService.getWatermark('c1');
            expect(watermark).toBeNull();
        });

        it('should return the date if watermark found', async () => {
            const date = new Date();
            pool.query.mockResolvedValueOnce({ rows: [{ last_exported_at: date }] });
            const watermark = await exportService.getWatermark('c1');
            expect(watermark).toBe(date);
        });
    });

    describe('executeExport', () => {
        it('should fail gracefully if DB connection fails', async () => {
            pool.connect.mockRejectedValueOnce(new Error('DB error'));
            await exportService.runFullExport('j1', 'c1', 'o.csv');
            expect(pool.connect).toHaveBeenCalled();
        });

        it('should execute full export correctly', async () => {
            pool.connect.mockResolvedValueOnce(mockClient);
            pool.query.mockResolvedValueOnce({ rows: [] }); // No existing watermark

            // Mock first chunk with 1 row, second chunk empty
            mockClient.query
                .mockResolvedValueOnce({ rows: [{ id: 1, numeric_id: 1, name: 'A', email: 'a@b.c', created_at: new Date(), updated_at: new Date(), is_deleted: false }] })
                .mockResolvedValueOnce({ rows: [] })
                .mockResolvedValueOnce({ rows: [] }) // for COMMIT
                .mockResolvedValueOnce({ rows: [] }); // ...

            await exportService.runFullExport('j2', 'c2', 'f.csv');
            expect(mockClient.release).toHaveBeenCalled();
        });

        it('should execute incremental export with watermark', async () => {
            pool.connect.mockResolvedValueOnce(mockClient);
            pool.query.mockResolvedValueOnce({ rows: [{ last_exported_at: new Date() }] });

            mockClient.query
                .mockResolvedValueOnce({ rows: [{ id: 2, numeric_id: 2, name: 'B', email: 'b@b.c', created_at: new Date(), updated_at: new Date(), is_deleted: false }] })
                .mockResolvedValueOnce({ rows: [] })
                .mockResolvedValueOnce({ rows: [] })
                .mockResolvedValueOnce({ rows: [] });

            await exportService.runIncrementalExport('j3', 'c3', 'i.csv');
            expect(mockClient.release).toHaveBeenCalled();
        });

        it('should execute delta export correctly without watermark', async () => {
            pool.connect.mockResolvedValueOnce(mockClient);
            pool.query.mockResolvedValueOnce({ rows: [] }); // No existing watermark

            mockClient.query
                .mockResolvedValueOnce({ rows: [{ id: 1, numeric_id: 1, name: 'A', email: 'a@b.c', created_at: new Date(), updated_at: new Date(), is_deleted: false }] })
                .mockResolvedValueOnce({ rows: [] })
                .mockResolvedValueOnce({ rows: [] })
                .mockResolvedValueOnce({ rows: [] });

            await exportService.runDeltaExport('j4', 'c4', 'd.csv');
            expect(mockClient.release).toHaveBeenCalled();
        });

        it('should catch error during extraction and rollback', async () => {
            pool.connect.mockResolvedValueOnce(mockClient);
            pool.query.mockRejectedValueOnce(new Error('Query failed')); // fail getting watermark

            mockClient.query.mockRejectedValueOnce(new Error('Rollback failed')); // mock rollback throw

            await exportService.runFullExport('j5', 'c5', 'err.csv');
            expect(mockClient.release).toHaveBeenCalled();
        });
    });
});
