CREATE TABLE galaxies
(
    id              SERIAL PRIMARY KEY NOT NULL,
    source_id       VARCHAR            NOT NULL UNIQUE,
    ra              DECIMAL(25, 20)    NOT NULL,
    dec             DECIMAL(25, 20)    NOT NULL,
    gal_prob        DECIMAL(25, 20)    NOT NULL,
    bin_id          SMALLINT           NOT NULL,
    status          VARCHAR            NOT NULL DEFAULT 'Pending',
    failed_attempts SMALLINT           NOT NULL DEFAULT 0
);

CREATE TABLE bands
(
    source_id   VARCHAR NOT NULL,
    band        CHAR    NOT NULL,
    error_count INT     NOT NULL DEFAULT 0,
    PRIMARY KEY (source_id, band),
    FOREIGN KEY (source_id) REFERENCES galaxies (source_id) ON DELETE CASCADE,
    CHECK (band IN ('g', 'r', 'i', 'z'))
);

CREATE TABLE rotations
(
    source_id     VARCHAR NOT NULL,
    band          CHAR    NOT NULL,
    degree        INT     NOT NULL,
    total_error   FLOAT DEFAULT 0,
    running_count INT   DEFAULT 0,
    PRIMARY KEY (source_id, band),
    FOREIGN KEY (source_id, band) REFERENCES bands (source_id, band) ON DELETE CASCADE
);

-- Unused
CREATE TABLE ellipticity
(
    source_id VARCHAR NOT NULL,
    band      CHAR    NOT NULL,
    PRIMARY KEY (source_id, band),
    FOREIGN KEY (source_id, band) REFERENCES bands (source_id, band) ON DELETE CASCADE
);
