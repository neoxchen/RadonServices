CREATE TABLE metadata
(
    schema_version SMALLINT PRIMARY KEY NOT NULL
);

CREATE TABLE galaxies
(
    uid             SERIAL          NOT NULL PRIMARY KEY,
    source_id       VARCHAR         NOT NULL UNIQUE,
    ra              DECIMAL(25, 20) NOT NULL,
    dec             DECIMAL(25, 20) NOT NULL,
    gal_prob        DECIMAL(25, 20) NOT NULL,
    status          VARCHAR         NOT NULL DEFAULT 'Pending',
    failed_attempts SMALLINT        NOT NULL DEFAULT 0,
    CHECK (status IN ('Pending', 'Success', 'Failed'))
);

CREATE TABLE bands
(
    uid        SERIAL   NOT NULL PRIMARY KEY,
    source_id  VARCHAR  NOT NULL,
    band       CHAR     NOT NULL,
    bin_id     VARCHAR  NOT NULL,
    batch_id   VARCHAR  NOT NULL,
    fits_index SMALLINT NOT NULL,
    has_error  BOOLEAN  NOT NULL DEFAULT FALSE,
    FOREIGN KEY (source_id) REFERENCES galaxies (source_id) ON DELETE CASCADE
);

CREATE TABLE rotations
(
    band_uid      INT           NOT NULL PRIMARY KEY,
    has_data      BOOLEAN       NOT NULL DEFAULT FALSE,
    degree        DECIMAL(7, 4) NOT NULL,
    total_error   FLOAT         NOT NULL DEFAULT 0,
    running_count INT           NOT NULL DEFAULT 0,
    FOREIGN KEY (band_uid) REFERENCES bands (uid) ON DELETE CASCADE
);

-- Used in augmentation pipeline
CREATE VIEW augment_view AS
SELECT b.uid,
       b.source_id,
       b.bin_id,
       b.batch_id,
       b.band,
       b.fits_index
FROM bands b
         JOIN rotations r ON b.uid = r.band_uid
WHERE b.has_error = FALSE
  AND r.has_data = TRUE
  AND r.running_count < 100;

-- TODO: Galaxy Details View: combined data from galaxies, bands and rotations???
CREATE VIEW galaxy_details AS
SELECT g.id            AS galaxy_id,
       g.source_id     AS galaxy_source_id,
       g.ra            AS galaxy_ra,
       g.dec           AS galaxy_dec,
       g.bin_id        AS galaxy_bin_id,
       g.status        AS galaxy_status,
       b.band          AS band_band,
       b.error_count   AS band_error_count,
       r.degree        AS rotations_degree,
       r.total_error   AS rotations_total_error,
       r.running_count AS rotations_running_count
FROM galaxies g
         JOIN bands b ON g.source_id = b.source_id
         LEFT JOIN rotations r ON b.source_id = r.source_id AND b.band = r.band;

-- Unused for now
CREATE TABLE ellipticity
(
    band_uid INT     NOT NULL PRIMARY KEY,
    has_data BOOLEAN NOT NULL DEFAULT FALSE,
    FOREIGN KEY (band_uid) REFERENCES bands (uid) ON DELETE CASCADE
);
