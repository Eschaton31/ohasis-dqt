SELECT *
FROM ohasis_interim.px_faci
WHERE REC_ID IN (
    SELECT REC_ID
    FROM ohasis_interim.px_faci
    WHERE SERVICE_TYPE <> '101102'
    GROUP BY REC_ID
    HAVING COUNT(*) > 1
)