SELECT *
FROM (SELECT data.CENTRAL_ID,
             data.DATE_CONFIRM,
             data.CONFIRM_RESULT,
             data.CONFIRM_REMARKS,
             ROW_NUMBER() OVER (PARTITION BY CENTRAL_ID ORDER BY DATE_CONFIRM DESC) AS VISIT_NUM
      FROM (SELECT COALESCE(reg.CENTRAL_ID, rec.PATIENT_ID)                                    AS CENTRAL_ID,
                   DATE(COALESCE(data.DATE_CONFIRM, data.T3_DATE, data.T2_DATE, data.T1_DATE)) AS DATE_CONFIRM,
                   data.CONFIRM_RESULT,
                   data.CONFIRM_REMARKS
            FROM ohasis_lake.px_pii AS rec
                     LEFT JOIN ohasis_warehouse.id_registry AS reg ON rec.PATIENT_ID = reg.PATIENT_ID
                     LEFT JOIN ohasis_lake.px_hiv_testing AS data ON rec.REC_ID = data.REC_ID
            WHERE rec.DELETED_AT IS NULL
              AND DATE(COALESCE(data.DATE_CONFIRM, data.T3_DATE, data.T2_DATE, data.T1_DATE)) <= ?
              AND data.CONFIRM_RESULT <> '4_Pending') AS data) AS artstart
WHERE VISIT_NUM = 1;