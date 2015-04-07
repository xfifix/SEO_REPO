CREATE TABLE IF NOT EXISTS NODES (
    ID SERIAL PRIMARY KEY NOT NULL,
    CATEGORIE_LEVEL_4 TEXT,
    MAGASIN VARCHAR(100)
) TABLESPACE mydbspace;


CREATE INDEX ON NODES (CATEGORIE_LEVEL_4);
ALTER SEQUENCE nodes_pkey RESTART WITH 1


# name weight not created (optional)
CREATE TABLE IF NOT EXISTS EDGES (
    SOURCE INT,
    TARGET INT
) TABLESPACE mydbspace;
