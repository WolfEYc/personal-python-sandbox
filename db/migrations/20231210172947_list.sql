-- migrate:up
CREATE TABLE list (
    id INTEGER PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    creator_id INTEGER NOT NULL REFERENCES client(id)
);

-- migrate:down
DROP TABLE list;
