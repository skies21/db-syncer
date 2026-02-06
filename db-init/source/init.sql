CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    name TEXT,
    age INT,
    city TEXT
);

INSERT INTO users (email, name, age, city)
VALUES
('a@test.com', 'Alice', 25, 'London'),
('b@test.com', 'Bob', 30, 'Paris'),
('d@test.com', 'David', 40, 'Berlin'),
('e@test.com', 'Eve', 35, 'Rome');
