-- migrate:up
CREATE TABLE item (
    id INTEGER PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    price INTEGER
);

-- migrate:down
DROP TABLE item;
