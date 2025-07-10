SELECT
    c.label AS eacei,
    a.label AS activity,
    CAST(e.obs_value AS DECIMAL(10,2)) AS obs_value,
    u.label AS unit_mult,
    m.label AS unit_measure,
    g.label AS geo,
    t.label AS time_period
FROM
    elec_cons e
JOIN eacei_measure_labels c ON e.EACEI_MEASURE = c.code
JOIN activity_labels a ON e.activity = a.code
JOIN unit_mult_labels u ON e.unit_mult = u.code
JOIN unit_measure_labels m ON e.unit_measure = m.code
JOIN geo_labels g ON e.geo = g.code
JOIN time_period_labels t ON e.time_period = t.code
WHERE
    c.label LIKE '%Consommation%'
    AND a.label <> 'Total'
    AND CAST(e.obs_value AS DECIMAL(10,2)) = (
        SELECT MAX(CAST(e2.obs_value AS DECIMAL(10,2)))
        FROM elec_cons e2
        JOIN activity_labels a2 ON e2.activity = a2.code
        WHERE
            e2.EACEI_MEASURE = e.EACEI_MEASURE
            AND e2.geo = e.geo
            AND e2.time_period = e.time_period
            AND a2.label <> 'Total'
    )
ORDER BY
    obs_value DESC;
