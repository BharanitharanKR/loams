-- The login PgDog uses on every backend (scratch only).
CREATE ROLE app LOGIN PASSWORD 'app-pw';
GRANT ALL ON DATABASE postgres TO app;
