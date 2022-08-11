create table if not exists GEOLOC_DIMEN(
    coord_id INT NOT NULL PRIMARY KEY,
    coord_lon FLOAT,
    coord_la FLOAT,
    imzon TEXT,
    id TEXT,
    nam TEXT,
    cod TEXT
);