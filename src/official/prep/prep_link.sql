WITH hts_data AS (SELECT CENTRAL_ID,
                         RECORD_DATE AS HTS_DATE,
                         REC_ID      AS HTS_REC
                  FROM (SELECT REC_ID, RECORD_DATE, PATIENT_ID, FORM_VERSION
                        FROM ohasis_warehouse.form_hts
                        UNION
                        SELECT REC_ID, RECORD_DATE, PATIENT_ID, FORM_VERSION
                        FROM ohasis_warehouse.form_a
                        UNION
                        SELECT REC_ID, RECORD_DATE, PATIENT_ID, FORM_VERSION
                        FROM ohasis_warehouse.form_cfbs) AS hts
                           JOIN ohasis_warehouse.id_registry ON hts.PATIENT_ID = id_registry.PATIENT_ID),
     prep_data AS (SELECT prep.REC_ID                                AS PREP_REC,
                          IFNULL(id_reg.CENTRAL_ID, prep.PATIENT_ID) AS CENTRAL_ID,
                          prep.RECORD_DATE                           AS PREP_DATE
                   FROM ohasis_warehouse.form_prep AS prep
                            LEFT JOIN ohasis_warehouse.id_registry AS id_reg ON prep.PATIENT_ID = id_reg.PATIENT_ID
                   WHERE prep.REC_ID NOT IN (SELECT DESTINATION_REC from ohasis_warehouse.rec_link))

SELECT prep_data.CENTRAL_ID,
       hts_data.HTS_REC,
       prep_data.PREP_REC,
       CASE
           WHEN PREP_DATE = HTS_DATE THEN 1
           WHEN ABS(DATEDIFF(PREP_DATE, HTS_DATE)) <= 7 THEN 2
           ELSE 9999
           END                            AS sort,
       ABS(DATEDIFF(PREP_DATE, HTS_DATE)) AS days_from_test
FROM prep_data
         JOIN hts_data ON prep_data.CENTRAL_ID = hts_data.CENTRAL_ID
WHERE ABS(DATEDIFF(PREP_DATE, HTS_DATE)) <= 7;