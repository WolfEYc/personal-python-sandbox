-- migrate:up
CREATE TABLE client (
    id INTEGER PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    dob DATE
);

-- migrate:down
DROP TABLE client;
