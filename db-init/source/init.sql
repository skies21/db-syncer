CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    name TEXT,
    age INT,
    city TEXT,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT true
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    amount NUMERIC(10,2) NOT NULL
);

INSERT INTO users (id, email, name, age, city, last_login, is_active)
VALUES
(1, 'a@test.com', 'Alice', 25, 'London', '2024-01-01', true),
(2, 'b@test.com', 'Bob', 30, 'Paris', NULL, true),
(3, 'd@test.com', 'David', 40, 'Berlin', '2024-02-01', false),
(4, 'e@test.com', 'Eve', 35, 'Rome', NULL, true);
