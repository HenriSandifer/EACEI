SELECT
    c.label AS eacei,
    a.label AS activity,
    CAST(e.obs_value AS DECIMAL(10,2)) AS obs_value,
    u.label AS unit_mult,
    m.label AS unit_measure,
	t.label AS time_period,
    g.label AS geo
    
FROM
    elec_cons e
JOIN eacei_measure_labels c ON e.EACEI_MEASURE = c.code
JOIN activity_labels a ON e.activity = a.code
JOIN unit_mult_labels u ON e.unit_mult = u.code
JOIN unit_measure_labels m ON e.unit_measure = m.code
JOIN
    time_period_labels t ON e.time_period = t.code
JOIN
    geo_labels g ON e.geo = g.code
WHERE
    c.label LIKE '%Consommation%'
    AND (
        -- this subquery ensures you're only getting the MAX per EACEI label
        CAST(e.obs_value AS DECIMAL(10,2)) =
        (
            SELECT MAX(CAST(e2.obs_value AS DECIMAL(10,2)))
            FROM elec_cons e2
            WHERE e2.EACEI_MEASURE = e.EACEI_MEASURE
        )
    )

ORDER BY obs_value DESC