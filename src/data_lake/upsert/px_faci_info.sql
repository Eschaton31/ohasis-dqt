SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       MAX(CASE service.SERVICE_TYPE
               WHEN '*00001' THEN '*00001_Mortality'
               WHEN '101101' THEN '101101_Facility-based Testing (FBT)'
               WHEN '101103' THEN '101103_Community-based (CBS)'
               WHEN '101104' THEN '101104_Non-laboratory FBT (FBS)'
               WHEN '101105' THEN '101105_Self-testing'
               WHEN '101201' THEN '101201_Anti-Retroviral Treatment (ART)'
               WHEN '101301' THEN '101301_Pre-Exposure Prophylaxis (PrEP)'
               WHEN '101303' THEN '101303_Prevention of Mother-to-Child Transmission (PMTCT)'
               WHEN '101304' THEN '101304_Reach'
               WHEN '1**001' THEN '1**001_STIs'
               ELSE service.SERVICE_TYPE END)                                                          AS MODALITY,
       MAX(service.FACI_ID)                                                                            AS SERVICE_FACI,
       MAX(service.SUB_FACI_ID)                                                                        AS SERVICE_SUB_FACI,
       MAX(service.PROVIDER_ID)                                                                        AS SERVICE_BY,
       MAX(service.REFER_BY_ID)                                                                        AS REFER_FACI,
       MAX(CASE service.CLIENT_TYPE
               WHEN 1 THEN '1_Inpatient'
               WHEN 2 THEN '2_Walk-in / Outpatient'
               WHEN 3 THEN '3_Mobile HTS Client'
               WHEN 4 THEN '4_Referral'
               WHEN 5 THEN '5_Satellite Client'
               WHEN 6 THEN '6_Transient'
               WHEN 7 THEN '7_Persons Deprived of Liberty'
               WHEN 8 THEN '8_Courier'
               ELSE CLIENT_TYPE
           END)                                                                                        AS CLIENT_TYPE,
       MAX(CASE service.REFER_TYPE
               WHEN 1 THEN '1_TB-DOTS / PMDT Facility'
               WHEN 2 THEN '2_Antenatal / Maternity Clinic'
           END)                                                                                        AS REFER_TYPE,
       MAX(CASE service.PROVIDER_TYPE
               WHEN 1 THEN '1_Medical Technologist'
               WHEN 2 THEN '2_HIV Counselor'
               WHEN 3 THEN '3_CBS Motivator'
               WHEN 8888 THEN '8888_Other'
           END)                                                                                        AS PROVIDER_TYPE,
       MAX(service.PROVIDER_TYPE_OTHER)                                                                AS PROVIDER_TYPE_OTHER,
       MAX(CASE service.TX_STATUS
               WHEN 1 THEN '1_Enrollment'
               WHEN 2 THEN '2_Refill'
               WHEN 0 THEN '0_Not on ART'
           END)                                                                                        AS TX_STATUS,
       MAX(CASE service.VISIT_TYPE
               WHEN 1 THEN '1_First consult at this facility'
               WHEN 2 THEN '2_Follow-up'
               WHEN 3 THEN '3_Inpatient'
           END)                                                                                        AS VISIT_TYPE,
       MAX(IF(remarks.REMARK_TYPE = 1, remarks.REMARKS, NULL))                                         AS CLINIC_NOTES,
       MAX(IF(remarks.REMARK_TYPE = 2, remarks.REMARKS, NULL))                                         AS COUNSEL_NOTES,
       MAX(IF(remarks.REMARK_TYPE = 3, remarks.REMARKS, NULL))                                         AS REPORT_NOTES,
       MAX(IF(remarks.REMARK_TYPE = 10, remarks.REMARKS, NULL))                                        AS STI_DIAGNOSIS
FROM ohasis_interim.px_record AS rec
         LEFT JOIN ohasis_interim.px_faci AS service on rec.REC_ID = service.REC_ID AND service.SERVICE_TYPE <> '101102'
         LEFT JOIN ohasis_interim.px_remarks AS remarks on rec.REC_ID = remarks.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;