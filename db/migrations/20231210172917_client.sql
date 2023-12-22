-- migrate:up
CREATE TABLE client (
    id INTEGER PRIMARY KEY,
    first_name VARCHAR(64) NOT NULL,
    last_name VARCHAR(64) NOT NULL,
    dob DATE
);

-- migrate:down
DROP TABLE client;
