-- migrate:up
CREATE TABLE item_in_list (
    item_id INTEGER REFERENCES item (id),
    list_id INTEGER REFERENCES list (id),
    quantity INTEGER NOT NULL,
    PRIMARY KEY (item_id, list_id)
);

-- migrate:down
DROP TABLE item_in_list;
