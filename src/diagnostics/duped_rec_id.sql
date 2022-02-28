SELECT *
FROM ohasis_interim.px_record
WHERE REC_ID IN (SELECT REC_ID FROM ohasis_interim.px_record GROUP BY REC_ID HAVING COUNT(*) > 1)