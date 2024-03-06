CREATE TABLE IF NOT EXISTS galaxies
(
    id        SERIAL PRIMARY KEY NOT NULL,
    source_id VARCHAR            NOT NULL UNIQUE,
    ra        DECIMAL(25, 20)    NOT NULL,
    dec       DECIMAL(25, 20)    NOT NULL,
    gal_prob  DECIMAL(25, 20)    NOT NULL,
    bin       SMALLINT           NOT NULL
);

CREATE TABLE IF NOT EXISTS fetch_results
(
    id              SERIAL PRIMARY KEY NOT NULL,
    source_id       VARCHAR            NOT NULL UNIQUE REFERENCES galaxies (source_id),
    status          VARCHAR            NOT NULL DEFAULT 'Pending',
    failed_attempts SMALLINT           NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS bands
(
    id          SERIAL PRIMARY KEY NOT NULL,
    source_id   VARCHAR            NOT NULL REFERENCES galaxies (source_id),
    band        CHAR               NOT NULL,
    error_count INT                NOT NULL DEFAULT 0,
    UNIQUE (source_id, band)
);

CREATE TABLE IF NOT EXISTS rotations
(
    id            SERIAL PRIMARY KEY NOT NULL,
    band_id       INT                NOT NULL UNIQUE REFERENCES bands (id),
    degree        INT CHECK (degree >= 0 AND degree <= 180),
    total_error   FLOAT DEFAULT 0,
    running_count INT   DEFAULT 0
);

-- unused
CREATE TABLE IF NOT EXISTS sine_approximations
(
    id          SERIAL PRIMARY KEY NOT NULL,
    rotation_id INT                NOT NULL UNIQUE REFERENCES rotations (id),
    level       FLOAT              NOT NULL,
    a           FLOAT              NOT NULL,
    b           FLOAT              NOT NULL,
    c           FLOAT              NOT NULL,
    d           FLOAT              NOT NULL,
    rmse        FLOAT              NOT NULL,
    UNIQUE (rotation_id, level)
);

CREATE TABLE IF NOT EXISTS normal_distributions
(
    id          SERIAL PRIMARY KEY NOT NULL,
    rotation_id INT                NOT NULL UNIQUE REFERENCES rotations (id),
    window_size SMALLINT           NOT NULL,
    mean        FLOAT              NOT NULL,
    std_dev     FLOAT              NOT NULL,
    UNIQUE (rotation_id, window_size)
);
