SELECT DISTINCT
    c.label AS EACEI
FROM
    elec_cons e
JOIN
    eacei_measure_labels c ON e.EACEI_MEASURE = c.code

LIMIT 1000;
