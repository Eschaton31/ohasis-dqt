SELECT *
FROM ohasis_interim.px_faci
WHERE REC_ID IN (
    SELECT REC_ID
    FROM ohasis_interim.px_faci
    GROUP BY REC_ID
    HAVING COUNT(*) > 1
)
  AND SERVICE_TYPE NOT IN ('101102', '101101')