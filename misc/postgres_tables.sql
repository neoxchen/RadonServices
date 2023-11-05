CREATE TABLE galaxies
(
    id              SERIAL PRIMARY KEY NOT NULL,
    source_id       VARCHAR            NOT NULL UNIQUE,
    ra              DECIMAL(25, 20)    NOT NULL,
    dec             DECIMAL(25, 20)    NOT NULL,
    gal_prob        DECIMAL(25, 20)    NOT NULL,
    bin             SMALLINT           NOT NULL,
    status          VARCHAR            NOT NULL DEFAULT 'Pending',
    failed_attempts SMALLINT           NOT NULL DEFAULT 0
);

CREATE TABLE bands
(
    id            SERIAL PRIMARY KEY NOT NULL,
    source_id     VARCHAR REFERENCES galaxies (source_id),
    band          CHAR               NOT NULL,
    degree        INT CHECK (degree >= 0 AND degree <= 180),
    total_error   FLOAT DEFAULT 0,
    running_count INT   DEFAULT 0,
    UNIQUE (source_id, band)
);

CREATE TABLE sine_approximations
(
    id      SERIAL PRIMARY KEY NOT NULL,
    band_id INT REFERENCES bands (id),
    level   FLOAT              NOT NULL,
    a       FLOAT              NOT NULL,
    b       FLOAT              NOT NULL,
    c       FLOAT              NOT NULL,
    d       FLOAT              NOT NULL,
    rmse    FLOAT              NOT NULL,
    UNIQUE (band_id, level)
);

CREATE TABLE normal_distributions
(
    id          SERIAL PRIMARY KEY NOT NULL,
    band_id     INT REFERENCES bands (id),
    window_size SMALLINT           NOT NULL,
    mean        FLOAT              NOT NULL,
    std_dev     FLOAT              NOT NULL,
    UNIQUE (band_id, window_size)
);
