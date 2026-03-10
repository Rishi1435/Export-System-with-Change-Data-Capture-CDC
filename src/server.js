const app = require('./app');
const { logger } = require('./logger');
const { pool } = require('./db');

const PORT = process.env.PORT || 8080;

async function start() {
    try {
        // Verify DB connection before starting server
        await pool.query('SELECT 1');
        logger.info('Connected to the database');

        app.listen(PORT, () => {
            logger.info(`Server is running on port ${PORT}`);
        });
    } catch (error) {
        logger.error(error, 'Failed to start the server due to database connection error');
        process.exit(1);
    }
}

start();
