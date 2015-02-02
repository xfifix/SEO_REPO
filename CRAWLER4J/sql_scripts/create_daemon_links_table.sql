# x y size not created (optional), Label URL (optional)

CREATE TABLE IF NOT EXISTS NODES (
    ID SERIAL PRIMARY KEY NOT NULL,
    LABEL TEXT,
    MAGASIN VARCHAR(100),
    STATUS_CODE INT,
    PAGE_TYPE VARCHAR(50)
) TABLESPACE mydbspace;


CREATE INDEX ON NODES (label);
ALTER SEQUENCE nodes_pkey RESTART WITH 1


# name weight not created (optional)
CREATE TABLE IF NOT EXISTS EDGES (
    SOURCE INT,
    TARGET INT
) TABLESPACE mydbspace;


cleaning up the untrimmed database
update nodes set label = trim(label)



DROP TABLE IF EXISTS NODES;
DROP TABLE IF EXISTS EDGES;
