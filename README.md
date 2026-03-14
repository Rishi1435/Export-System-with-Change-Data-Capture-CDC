# CDC Data Export System

A production-ready, containerized backend API with a PostgreSQL database that streams large datasets asynchronously.

## Video Presentation
[Watch the Code Walkthrough on YouTube](https://youtu.be/xeuH-tKQoJI)

## Features
- Background execution of exports (full, incremental, delta).
- Robust pagination streaming data cleanly out of massive Postgres datasets.
- Watermark updating for Change Data Capture sequences.

## Running Locally

1. Create a `.env` file based on `.env.example`.
2. Run `docker-compose up --build`.
   - The Postgres db will create the tables and seed 100,000+ records.
   - The Node app is running on `http://localhost:8080`.

## Testing & Coverage

To run the unit/integration tests and check code coverage requirement met:

```bash
docker-compose run app npm run test:coverage
```

Or if you have dependencies installed locally:
```bash
npm install
npm run test:coverage
```

The system requires at least 70% code coverage.
