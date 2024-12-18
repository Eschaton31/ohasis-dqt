SELECT *
FROM (SELECT data.CENTRAL_ID,
             data.REC_ID,
             data.VISIT_DATE,
             ROW_NUMBER() OVER (PARTITION BY CENTRAL_ID ORDER BY VISIT_DATE DESC) AS VISIT_NUM
      FROM (
               SELECT CASE
                          WHEN reg.CENTRAL_ID IS NULL THEN rec.PATIENT_ID
                          WHEN reg.CENTRAL_ID IS NOT NULL THEN reg.CENTRAL_ID
                          END AS CENTRAL_ID,
                      rec.*
               FROM ohasis_warehouse.form_prep rec
                        LEFT JOIN ohasis_warehouse.id_registry reg ON rec.PATIENT_ID = reg.PATIENT_ID
               WHERE PREP_RECORD = 'PrEP' AND VISIT_DATE <= ? AND MEDICINE_SUMMARY IS NOT NULL
           ) AS data) AS prepstart
WHERE VISIT_NUM = 1;