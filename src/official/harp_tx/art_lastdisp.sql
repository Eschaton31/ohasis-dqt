SELECT *
FROM (SELECT data.CENTRAL_ID,
             data.REC_ID,
             data.VISIT_DATE,
             ROW_NUMBER() OVER (PARTITION BY CENTRAL_ID ORDER BY VISIT_DATE DESC) AS VISIT_NUM
      FROM (
               SELECT COALESCE(reg.CENTRAL_ID, rec.PATIENT_ID) AS CENTRAL_ID,
                      rec.*
               FROM ohasis_warehouse.form_art_bc AS rec
                        LEFT JOIN ohasis_warehouse.id_registry AS reg ON rec.PATIENT_ID = reg.PATIENT_ID
               WHERE MEDICINE_SUMMARY IS NOT NULL AND DATE(VISIT_DATE) <= ?
           ) AS data) AS artstart
WHERE VISIT_NUM = 1;