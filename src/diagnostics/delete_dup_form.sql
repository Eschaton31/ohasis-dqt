-- Drop dupe rec_id whose pair is missing
DELETE
FROM ohasis_interim.px_form
WHERE FORM = ''
  AND REC_ID IN (
    SELECT REC_ID
    FROM ohasis_interim.px_form
    WHERE REC_ID IN (
        SELECT REC_ID
        FROM ohasis_interim.px_form
        GROUP BY REC_ID
        HAVING COUNT(*) > 1
    )
  );
