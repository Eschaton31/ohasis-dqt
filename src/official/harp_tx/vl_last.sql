SELECT *
FROM (SELECT data.CENTRAL_ID,
             data.LAB_VIRAL_DATE,
             data.LAB_VIRAL_RESULT,
             ROW_NUMBER() OVER (PARTITION BY CENTRAL_ID ORDER BY LAB_VIRAL_DATE DESC) AS VISIT_NUM
      FROM (SELECT COALESCE(reg.CENTRAL_ID, rec.PATIENT_ID)      AS CENTRAL_ID,
                   COALESCE(rec.LAB_VIRAL_DATE, pii.RECORD_DATE) AS LAB_VIRAL_DATE,
                   rec.LAB_VIRAL_RESULT
            FROM ohasis_lake.lab_wide AS rec
                     LEFT JOIN ohasis_warehouse.id_registry AS reg ON rec.PATIENT_ID = reg.PATIENT_ID
                     LEFT JOIN ohasis_lake.px_pii AS pii ON rec.REC_ID = pii.REC_ID
            WHERE rec.DELETED_AT IS NULL
              AND rec.LAB_VIRAL_RESULT IS NOT NULL
              AND DATE(COALESCE(rec.LAB_VIRAL_DATE, pii.RECORD_DATE)) <= ?) AS data) AS artstart
WHERE VISIT_NUM = 1;