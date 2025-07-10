WITH ranked_activities AS (
    SELECT
        c.label AS eacei,
        a.label AS activity,
        CAST(e.obs_value AS DECIMAL(10,2)) AS obs_value,
        u.label AS unit_mult,
        m.label AS unit_measure,
        g.label AS geo,
        t.label AS time_period,
        ROW_NUMBER() OVER (
            PARTITION BY c.label, g.label, t.label
            ORDER BY CAST(e.obs_value AS DECIMAL(10,2)) DESC
        ) AS `rank`
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
)

SELECT *
FROM ranked_activities
WHERE `rank` <= 5
ORDER BY eacei, time_period, geo, `rank`;