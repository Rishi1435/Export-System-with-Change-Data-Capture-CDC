const { pool } = require('../db');
const { logger } = require('../logger');
const fs = require('fs');
const path = require('path');

const OUTPUT_DIR = path.join(process.cwd(), 'output');
const BATCH_SIZE = 10000;

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

    formatCsvValue(val) {
        if (val === null || val === undefined) return '';
        if (val instanceof Date) return val.toISOString();
        const str = String(val);
        if (str.includes(',') || str.includes('"') || str.includes('\n')) {
            return '"' + str.replace(/"/g, '""') + '"';
        }
        return str;
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
            // Fetch watermark using the acquired client
            const wmResult = await client.query(
                'SELECT last_exported_at FROM watermarks WHERE consumer_id = $1',
                [consumerId]
            );
            const lastExportedAt = wmResult.rows.length > 0 ? wmResult.rows[0].last_exported_at : null;

            let baseQuery = '';
            let queryParams = [];

            switch (exportType) {
                case 'full':
                    baseQuery = 'SELECT id, name, email, created_at, updated_at, is_deleted FROM users WHERE is_deleted = FALSE';
                    break;
                case 'incremental':
                    baseQuery = 'SELECT id, name, email, created_at, updated_at, is_deleted FROM users WHERE is_deleted = FALSE';
                    if (lastExportedAt) {
                        baseQuery += ' AND updated_at > $1';
                        queryParams.push(lastExportedAt);
                    }
                    break;
                case 'delta':
                    baseQuery = `SELECT id, name, email, created_at, updated_at, is_deleted,
                        CASE
                            WHEN is_deleted = TRUE THEN 'DELETE'
                            WHEN created_at >= updated_at THEN 'INSERT'
                            ELSE 'UPDATE'
                        END as operation
                        FROM users`;
                    if (lastExportedAt) {
                        baseQuery += ' WHERE updated_at > $1';
                        queryParams.push(lastExportedAt);
                    }
                    break;
            }

            baseQuery += ' ORDER BY updated_at ASC, id ASC';

            const filePath = path.join(OUTPUT_DIR, outputFilename);

            const columns = exportType === 'delta'
                ? ['operation', 'id', 'name', 'email', 'created_at', 'updated_at', 'is_deleted']
                : ['id', 'name', 'email', 'created_at', 'updated_at', 'is_deleted'];

            // Write CSV header
            fs.writeFileSync(filePath, columns.join(',') + '\n');

            let offset = 0;
            let hasMore = true;
            let rowsExported = 0;
            let maxUpdatedAt = null;

            while (hasMore) {
                const paginatedQuery = `${baseQuery} LIMIT ${BATCH_SIZE} OFFSET ${offset}`;
                const result = await client.query(paginatedQuery, queryParams);
                const rows = result.rows;

                if (rows.length === 0) {
                    hasMore = false;
                } else {
                    let csvChunk = '';
                    for (const row of rows) {
                        const line = columns.map(col => this.formatCsvValue(row[col])).join(',');
                        csvChunk += line + '\n';

                        if (!maxUpdatedAt || row.updated_at > maxUpdatedAt) {
                            maxUpdatedAt = row.updated_at;
                        }
                    }
                    fs.appendFileSync(filePath, csvChunk);

                    rowsExported += rows.length;
                    offset += rows.length;

                    if (rows.length < BATCH_SIZE) {
                        hasMore = false;
                    }
                }
            }

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
