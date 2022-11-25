SELECT *
FROM (SELECT data.CENTRAL_ID,
             data.REC_ID,
             data.VISIT_DATE,
             ROW_NUMBER() OVER (PARTITION BY CENTRAL_ID ORDER BY YEAR(VISIT_DATE) DESC, MONTH(VISIT_DATE) DESC, LATEST_NEXT_DATE DESC) AS VISIT_NUM
      FROM (
               SELECT CASE
                          WHEN reg.CENTRAL_ID IS NULL THEN rec.PATIENT_ID
                          WHEN reg.CENTRAL_ID IS NOT NULL THEN reg.CENTRAL_ID
                          END AS CENTRAL_ID,
                      rec.*
               FROM ohasis_warehouse.form_prep rec
                        LEFT JOIN ohasis_warehouse.id_registry reg ON rec.PATIENT_ID = reg.PATIENT_ID
                        LEFT JOIN ohasis_warehouse.prep_ignore ign ON rec.REC_ID = ign.REC_ID
               WHERE VISIT_DATE < ? AND ign.IGNORE IS NULL
           ) AS data) AS prepstart
WHERE VISIT_NUM = 1;