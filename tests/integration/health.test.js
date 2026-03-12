const request = require('supertest');
const app = require('../../src/app');

describe('Health Endpoint', () => {
    it('should return 200 OK and status ok', async () => {
        const res = await request(app).get('/health');
        expect(res.status).toBe(200);
        expect(res.body).toHaveProperty('status', 'ok');
        expect(res.body).toHaveProperty('timestamp');
    });
});
