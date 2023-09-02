CREATE TABLE projects (
    id SERIAL PRIMARY KEY,
    code VARCHAR(255),
    parent_code VARCHAR(255),
    name VARCHAR(255),
    version INT,
    data JSON DEFAULT '{}'
);