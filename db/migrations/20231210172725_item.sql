-- migrate:up
CREATE TABLE item (
    id INTEGER PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    price NUMERIC(10,2)
);

-- migrate:down
DROP TABLE item;
