SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       MAX(IF(vax.DISEASE_VAX = 'COVID19' AND vax.VAX_NUM = 1, vax.IS_VAX, NULL))                      AS VAX_COVID19_1ST_DONE,
       MAX(IF(vax.DISEASE_VAX = 'COVID19' AND vax.VAX_NUM = 1, vax.VAX_USED,
              NULL))                                                                                   AS VAX_COVID19_1ST_VAX_USED,
       MAX(IF(vax.DISEASE_VAX = 'COVID19' AND vax.VAX_NUM = 1, vax.VAX_DATE,
              NULL))                                                                                   AS VAX_COVID19_1ST_DATE,
       MAX(IF(vax.DISEASE_VAX = 'COVID19' AND vax.VAX_NUM = 1, vax.VAX_REMARKS,
              NULL))                                                                                   AS VAX_COVID19_1ST_LOCATION,
       MAX(IF(vax.DISEASE_VAX = 'COVID19' AND vax.VAX_NUM = 2, vax.IS_VAX, NULL))                      AS VAX_COVID19_2ND_DONE,
       MAX(IF(vax.DISEASE_VAX = 'COVID19' AND vax.VAX_NUM = 2, vax.VAX_USED,
              NULL))                                                                                   AS VAX_COVID19_2ND_VAX_USED,
       MAX(IF(vax.DISEASE_VAX = 'COVID19' AND vax.VAX_NUM = 2, vax.VAX_DATE,
              NULL))                                                                                   AS VAX_COVID19_2ND_DATE,
       MAX(IF(vax.DISEASE_VAX = 'COVID19' AND vax.VAX_NUM = 2, vax.VAX_REMARKS,
              NULL))                                                                                   AS VAX_COVID19_2ND_LOCATION,
       MAX(IF(vax.DISEASE_VAX = 'COVID19' AND vax.VAX_NUM = 3, vax.IS_VAX, NULL))                      AS VAX_COVID19_3RD_DONE,
       MAX(IF(vax.DISEASE_VAX = 'COVID19' AND vax.VAX_NUM = 3, vax.VAX_USED,
              NULL))                                                                                   AS VAX_COVID19_3RD_VAX_USED,
       MAX(IF(vax.DISEASE_VAX = 'COVID19' AND vax.VAX_NUM = 3, vax.VAX_DATE,
              NULL))                                                                                   AS VAX_COVID19_3RD_DATE,
       MAX(IF(vax.DISEASE_VAX = 'COVID19' AND vax.VAX_NUM = 3, vax.VAX_REMARKS,
              NULL))                                                                                   AS VAX_COVID19_3RD_LOCATION,
       MAX(IF(vax.DISEASE_VAX = 'COVID19' AND vax.VAX_NUM = 10, vax.IS_VAX, NULL))                     AS VAX_COVID19_BOOST_DONE,
       MAX(IF(vax.DISEASE_VAX = 'COVID19' AND vax.VAX_NUM = 10, vax.VAX_USED,
              NULL))                                                                                   AS VAX_COVID19_BOOST_VAX_USED,
       MAX(IF(vax.DISEASE_VAX = 'COVID19' AND vax.VAX_NUM = 10, vax.VAX_DATE,
              NULL))                                                                                   AS VAX_COVID19_BOOST_DATE,
       MAX(IF(vax.DISEASE_VAX = 'COVID19' AND vax.VAX_NUM = 10, vax.VAX_REMARKS,
              NULL))                                                                                   AS VAX_COVID19_BOOST_LOCATION,
       MAX(IF(vax.DISEASE_VAX = 'HEPB' AND vax.VAX_NUM = 1, vax.IS_VAX, NULL))                         AS VAX_HEPB_1ST_DONE,
       MAX(IF(vax.DISEASE_VAX = 'HEPB' AND vax.VAX_NUM = 1, vax.VAX_USED, NULL))                       AS VAX_HEPB_1ST_VAX_USED,
       MAX(IF(vax.DISEASE_VAX = 'HEPB' AND vax.VAX_NUM = 1, vax.VAX_DATE, NULL))                       AS VAX_HEPB_1ST_DATE,
       MAX(IF(vax.DISEASE_VAX = 'HEPB' AND vax.VAX_NUM = 1, vax.VAX_REMARKS,
              NULL))                                                                                   AS VAX_HEPB_1ST_LOCATION,
       MAX(IF(vax.DISEASE_VAX = 'HEPB' AND vax.VAX_NUM = 2, vax.IS_VAX, NULL))                         AS VAX_HEPB_2ND_DONE,
       MAX(IF(vax.DISEASE_VAX = 'HEPB' AND vax.VAX_NUM = 2, vax.VAX_USED, NULL))                       AS VAX_HEPB_2ND_VAX_USED,
       MAX(IF(vax.DISEASE_VAX = 'HEPB' AND vax.VAX_NUM = 2, vax.VAX_DATE, NULL))                       AS VAX_HEPB_2ND_DATE,
       MAX(IF(vax.DISEASE_VAX = 'HEPB' AND vax.VAX_NUM = 2, vax.VAX_REMARKS,
              NULL))                                                                                   AS VAX_HEPB_2ND_LOCATION,
       MAX(IF(vax.DISEASE_VAX = 'HEPB' AND vax.VAX_NUM = 3, vax.IS_VAX, NULL))                         AS VAX_HEPB_3RD_DONE,
       MAX(IF(vax.DISEASE_VAX = 'HEPB' AND vax.VAX_NUM = 3, vax.VAX_USED, NULL))                       AS VAX_HEPB_3RD_VAX_USED,
       MAX(IF(vax.DISEASE_VAX = 'HEPB' AND vax.VAX_NUM = 3, vax.VAX_DATE, NULL))                       AS VAX_HEPB_3RD_DATE,
       MAX(IF(vax.DISEASE_VAX = 'HEPB' AND vax.VAX_NUM = 3, vax.VAX_REMARKS,
              NULL))                                                                                   AS VAX_HEPB_3RD_LOCATION
FROM ohasis_interim.px_record AS rec
         JOIN (SELECT REC_ID,
                      CASE DISEASE_VAX
                          WHEN '102000' THEN 'HEPB'
                          WHEN '201000' THEN 'COVID19'
                          END           AS DISEASE_VAX,
                      CASE IS_VAX
                          WHEN 1 THEN '1_Yes'
                          WHEN 2 THEN '2_Refused'
                          WHEN 0 THEN '0_No'
                          ELSE NULL END AS IS_VAX,
                      VAX_NUM,
                      VAX_USED,
                      VAX_DATE,
                      VAX_REMARKS,
                      VAX_RESULT
               FROM ohasis_interim.px_vaccine) AS vax ON rec.REC_ID = vax.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
