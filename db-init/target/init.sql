CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    name TEXT,
    age TEXT,
    city TEXT,
    legacy_code TEXT
);

CREATE TABLE audit_log (
    id SERIAL PRIMARY KEY,
    message TEXT NOT NULL
);

INSERT INTO users (id, email, name, age, city, legacy_code)
VALUES
(1, 'a@test.com', 'Alice PROD', '99', 'London PROD', 'L1'),
(2, 'b@test.com', 'Bob PROD', NULL, NULL, 'L2'),
(3, 'c@test.com', 'Charlie', '30', 'Madrid', 'L3'),
(4, 'd@test.com', 'David PROD', '18', 'Berlin PROD', 'L4');
