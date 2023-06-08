SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       MAX(IF(other_service.SERVICE = 1013, GIVEN, NULL))                                              AS SERVICE_HIV_101,
       MAX(IF(other_service.SERVICE = 1004, GIVEN, NULL))                                              AS SERVICE_IEC_MATS,
       MAX(IF(other_service.SERVICE = 1002, GIVEN, NULL))                                              AS SERVICE_RISK_COUNSEL,
       MAX(IF(other_service.SERVICE = 5001, GIVEN, NULL))                                              AS SERVICE_PREP_REFER,
       MAX(IF(other_service.SERVICE = 5002, GIVEN, NULL))                                              AS SERVICE_SSNT_OFFER,
       MAX(IF(other_service.SERVICE = 5003, GIVEN, NULL))                                              AS SERVICE_SSNT_ACCEPT,
       MAX(IF(other_service.SERVICE = 2001, OTHER_SERVICE, NULL))                                      AS SERVICE_GIVEN_CONDOMS,
       MAX(IF(other_service.SERVICE = 2002, OTHER_SERVICE, NULL))                                      AS SERVICE_GIVEN_LUBES
FROM ohasis_interim.px_record AS rec
         JOIN (SELECT REC_ID,
                      SERVICE,
                      CASE GIVEN
                          WHEN 1 THEN '1_Yes'
                          WHEN 0 THEN '0_No'
                          ELSE NULL END AS GIVEN,
                      OTHER_SERVICE
               FROM ohasis_interim.px_other_service) AS other_service ON rec.REC_ID = other_service.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
