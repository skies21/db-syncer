CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    name TEXT,
    age INT,
    city TEXT
);

INSERT INTO users (id, email, name, age, city)
VALUES
(1, 'a@test.com', 'Alice PROD', 25, 'London PROD'),
(2, 'b@test.com', 'Bob PROD', NULL, NULL),
(3, 'c@test.com', 'Charlie', 30, 'Madrid');
