SELECT IFNULL(id_reg.CENTRAL_ID, prep.PATIENT_ID) AS CENTRAL_ID,
       REC_ID,
       VISIT_DATE,
       LATEST_NEXT_DATE,
       MEDICINE_SUMMARY
FROM ohasis_warehouse.form_prep AS prep
         LEFT JOIN ohasis_warehouse.id_registry AS id_reg ON prep.PATIENT_ID = id_reg.PATIENT_ID
WHERE PREP_RECORD = 'PrEP'
  AND (VISIT_DATE BETWEEN ? AND ?);