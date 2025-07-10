SELECT
    a.label AS activity,
    u.label AS unit_mult,
    m.label AS unit_measure,
    t.label AS time_period,
    g.label AS geo,
    c.label AS EACEI,
    CAST(e.obs_value AS DECIMAL(10,2)) AS obs_value
FROM
    my_project.elec_cons e
JOIN
    my_project.activity_labels a ON e.activity = a.code
JOIN
    my_project.unit_mult_labels u ON e.unit_mult = u.code
JOIN
    my_project.unit_measure_labels m ON e.unit_measure = m.code
JOIN
    my_project.time_period_labels t ON e.time_period = t.code
JOIN
    my_project.geo_labels g ON e.geo = g.code
JOIN
    my_project.eacei_measure_labels c ON e.EACEI_MEASURE = c.code
WHERE
    (
        c.label LIKE '%Électricité%'
        OR c.label LIKE '%facture énergétique%'
        OR c.label LIKE "%valeur des achats d'électricité%"
        OR c.label LIKE "%quantités achetées%"
    )
    AND
    (
        a.label = 'Métallurgie'
        OR a.label LIKE '%Denrées alimentaires%'
        OR a.label LIKE '%chimique%'
    )
    AND NOT a.label = 'Total'
ORDER BY
    CAST(e.obs_value AS DECIMAL(10,2)) DESC
LIMIT 1000;
