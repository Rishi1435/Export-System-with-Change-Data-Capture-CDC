CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_users_updated_at ON users(updated_at);

CREATE TABLE IF NOT EXISTS watermarks (
    id SERIAL PRIMARY KEY,
    consumer_id VARCHAR(255) NOT NULL UNIQUE,
    last_exported_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

DO $$
DECLARE
    users_count INT;
BEGIN
    SELECT COUNT(*) INTO users_count FROM users;
    
    IF users_count < 100000 THEN
        INSERT INTO users (name, email, created_at, updated_at, is_deleted)
        SELECT 
            'User ' || i,
            'user' || i || '_' || encode(gen_random_bytes(4), 'hex') || '@example.com',
            NOW() - ((random() * 7 + 1) || ' days')::interval,
            NOW() - ((random() * 7) || ' days')::interval,
            random() < 0.015
        FROM generate_series(1, 100000) AS i;
        
        -- Fix cases where updated_at < created_at
        UPDATE users 
        SET updated_at = created_at + (random() * interval '1 day')
        WHERE updated_at < created_at;

        RAISE NOTICE 'Seeded 100,000 users.';
    ELSE
        RAISE NOTICE 'Users table already populated (count: %)', users_count;
    END IF;
END $$;
