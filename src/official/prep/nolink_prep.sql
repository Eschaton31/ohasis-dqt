SELECT COALESCE(id_reg.CENTRAL_ID, prep.PATIENT_ID) AS CENTRAL_ID,
       prep.REC_ID                                  AS PREP_REC,
       prep.RECORD_DATE                             AS PREP_DATE
FROM ohasis_warehouse.form_prep AS prep
         LEFT JOIN ohasis_warehouse.id_registry AS id_reg ON prep.PATIENT_ID = id_reg.PATIENT_ID
         LEFT JOIN ohasis_warehouse.rec_link AS link ON prep.REC_ID = link.DESTINATION_REC
WHERE link.SOURCE_REC IS NULL