const { pool } = require('../db');
const { logger } = require('../logger');
const { createWriteStream } = require('fs');
const { stringify } = require('csv-stringify');
const path = require('path');

const OUTPUT_DIR = path.join(process.cwd(), 'output');

class ExportService {

    async getWatermark(consumerId) {
        const result = await pool.query(
            'SELECT last_exported_at FROM watermarks WHERE consumer_id = $1',
            [consumerId]
        );
        if (result.rows.length === 0) {
            return null;
        }
        return result.rows[0].last_exported_at;
    }

    async runFullExport(jobId, consumerId, outputFilename) {
        return this.executeExport(jobId, consumerId, outputFilename, 'full');
    }

    async runIncrementalExport(jobId, consumerId, outputFilename) {
        return this.executeExport(jobId, consumerId, outputFilename, 'incremental');
    }

    async runDeltaExport(jobId, consumerId, outputFilename) {
        return this.executeExport(jobId, consumerId, outputFilename, 'delta');
    }

    async executeExport(jobId, consumerId, outputFilename, exportType) {
        const startTime = Date.now();
        logger.info({ jobId, consumerId, exportType }, 'Export job started');

        let client;
        try {
            client = await pool.connect();
        } catch (error) {
            logger.error({ jobId, error: error.message }, 'Export job failed');
            return;
        }

        try {
            const lastExportedAt = await this.getWatermark(consumerId);

            let baseQuery = '';
            let queryParams = [];

            switch (exportType) {
                case 'full':
                    baseQuery = 'SELECT *, id as numeric_id FROM users WHERE is_deleted = FALSE';
                    break;
                case 'incremental':
                    baseQuery = 'SELECT *, id as numeric_id FROM users WHERE is_deleted = FALSE';
                    if (lastExportedAt) {
                        baseQuery += ' AND updated_at > $1';
                        queryParams.push(lastExportedAt);
                    }
                    break;
                case 'delta':
                    baseQuery = `
            SELECT *, id as numeric_id,
              CASE
                WHEN is_deleted = TRUE THEN 'DELETE'
                WHEN created_at >= updated_at THEN 'INSERT'
                ELSE 'UPDATE'
              END as operation
            FROM users 
          `;
                    if (lastExportedAt) {
                        baseQuery += ' WHERE updated_at > $1';
                        queryParams.push(lastExportedAt);
                    }
                    break;
            }

            const filePath = path.join(OUTPUT_DIR, outputFilename);
            const writeStream = createWriteStream(filePath);

            const columns = exportType === 'delta'
                ? ['operation', 'id', 'name', 'email', 'created_at', 'updated_at', 'is_deleted']
                : ['id', 'name', 'email', 'created_at', 'updated_at', 'is_deleted'];

            const stringifier = stringify({ header: true, columns, cast: { date: (value) => value.toISOString() } });
            stringifier.pipe(writeStream);

            let lastUpdatedAtPaging = null;
            let lastIdPaging = null;
            let hasMore = true;
            let rowsExported = 0;
            let maxUpdatedAt = null;

            while (hasMore) {
                let chunkQuery = baseQuery;
                let chunkParams = [...queryParams];

                // Add cursor conditions
                if (lastUpdatedAtPaging && lastIdPaging) {
                    if (chunkParams.length === 0) {
                        chunkQuery += ' WHERE';
                    } else {
                        if (exportType === 'full' || exportType === 'incremental') {
                            chunkQuery += ' AND';
                        } else {
                            if (!baseQuery.includes('WHERE')) {
                                chunkQuery += ' WHERE';
                            } else {
                                chunkQuery += ' AND';
                            }
                        }
                    }
                    chunkQuery += ` (updated_at > $${chunkParams.length + 1} OR (updated_at = $${chunkParams.length + 1} AND id > $${chunkParams.length + 2}))`;
                    chunkParams.push(lastUpdatedAtPaging, lastIdPaging);
                }

                chunkQuery += ' ORDER BY updated_at ASC, id ASC LIMIT 10000';

                const result = await client.query(chunkQuery, chunkParams);
                const rows = result.rows;

                if (rows.length === 0) {
                    hasMore = false;
                } else {
                    for (const row of rows) {
                        const outRow = {};
                        for (const col of columns) {
                            outRow[col] = row[col];
                        }
                        stringifier.write(outRow);

                        if (!maxUpdatedAt || row.updated_at > maxUpdatedAt) {
                            maxUpdatedAt = row.updated_at;
                        }
                    }

                    rowsExported += rows.length;
                    lastUpdatedAtPaging = rows[rows.length - 1].updated_at;
                    lastIdPaging = rows[rows.length - 1].numeric_id;
                }
            }

            stringifier.end();

            await new Promise((resolve, reject) => {
                writeStream.on('finish', () => resolve());
                writeStream.on('error', reject);
            });

            // Transactionally update the watermark
            if (maxUpdatedAt) {
                await client.query('BEGIN');
                await client.query(
                    `INSERT INTO watermarks (consumer_id, last_exported_at, updated_at) 
           VALUES ($1, $2, NOW()) 
           ON CONFLICT (consumer_id) 
           DO UPDATE SET last_exported_at = EXCLUDED.last_exported_at, updated_at = NOW()`,
                    [consumerId, maxUpdatedAt]
                );
                await client.query('COMMIT');
            }

            const duration = Date.now() - startTime;
            logger.info({ jobId, rowsExported, duration }, 'Export job completed');

        } catch (error) {
            if (client) {
                try { await client.query('ROLLBACK'); } catch (e) { }
            }
            logger.error({ jobId, error: error.message }, 'Export job failed');
        } finally {
            client.release();
        }
    }
}

const exportService = new ExportService();
module.exports = { exportService, ExportService };
