SELECT g.id AS galaxy_id,
       g.source_id,
       g.ra,
       g.dec,
       g.gal_prob,
       g.bin,
       g.status,
       g.failed_attempts,
       b.id AS band_id,
       b.band,
       b.degree,
       s.level,
       s.a,
       s.b,
       s.c,
       s.d,
       s.rmse,
       n.window,
       n.mean,
       n.std_dev
FROM galaxies g
         JOIN
     bands b ON g.source_id = b.source_id
         LEFT JOIN
     sine_approximations s ON b.id = s.band_id
         LEFT JOIN
     normal_distributions n ON b.id = n.band_id
WHERE g.source_id = 'Your desired source_id';
