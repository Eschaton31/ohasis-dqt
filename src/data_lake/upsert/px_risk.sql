SELECT exp_data.REC_ID,
       exp_data.CREATED_AT,
       exp_data.UPDATED_AT,
       exp_data.DELETED_AT,
       GREATEST(COALESCE(exp_data.DELETED_AT, 0), COALESCE(exp_data.UPDATED_AT, 0),
                COALESCE(exp_data.CREATED_AT, 0))                                                       AS SNAPSHOT,

       -- hiv mother
       MAX(IF(exp_data.EXPOSURE = 'HIV_MOTHER', exp_data.IS_EXPOSED, NULL))                             AS RISK_HIV_MOTHER,
       MAX(IF(exp_data.EXPOSURE = 'HIV_MOTHER', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_HIV_MOTHER_DATE,

       -- sex w/ males
       MAX(IF(exp_data.EXPOSURE = 'SEX_M', exp_data.IS_EXPOSED, NULL))                                  AS RISK_SEX_M,
       MAX(IF(exp_data.EXPOSURE = 'SEX_M', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_SEX_M_DATE,
       MAX(IF(exp_data.EXPOSURE = 'SEX_M_AV', exp_data.IS_EXPOSED, NULL))                               AS RISK_SEX_M_AV,
       MAX(IF(exp_data.EXPOSURE = 'SEX_M_AV', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_SEX_M_AV_DATE,
       MAX(IF(exp_data.EXPOSURE IN ('SEX_M_NOCONDOM', 'SEX_M_AV_NOCONDOM'), exp_data.IS_EXPOSED,
              NULL))                                                                                    AS RISK_SEX_M_AV_NOCONDOM,
       MAX(IF(exp_data.EXPOSURE IN ('SEX_M_NOCONDOM', 'SEX_M_AV_NOCONDOM'), DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_SEX_M_AV_NOCONDOM_DATE,

       -- sex w/ females
       MAX(IF(exp_data.EXPOSURE = 'SEX_F', exp_data.IS_EXPOSED, NULL))                                  AS RISK_SEX_F,
       MAX(IF(exp_data.EXPOSURE = 'SEX_F', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_SEX_F_DATE,
       MAX(IF(exp_data.EXPOSURE = 'SEX_F_AV', exp_data.IS_EXPOSED, NULL))                               AS RISK_SEX_F_AV,
       MAX(IF(exp_data.EXPOSURE = 'SEX_F_AV', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_SEX_F_AV_DATE,
       MAX(IF(exp_data.EXPOSURE IN ('SEX_F_NOCONDOM', 'SEX_F_AV_NOCONDOM'), exp_data.IS_EXPOSED,
              NULL))                                                                                    AS RISK_SEX_F_AV_NOCONDOM,
       MAX(IF(exp_data.EXPOSURE IN ('SEX_F_NOCONDOM', 'SEX_F_AV_NOCONDOM'), DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_SEX_F_AV_NOCONDOM_DATE,

       -- condomless sex
       MAX(IF(exp_data.EXPOSURE = 'CONDOMLESS_ANAL', exp_data.IS_EXPOSED, NULL))                        AS RISK_CONDOMLESS_ANAL,
       MAX(IF(exp_data.EXPOSURE = 'CONDOMLESS_ANAL', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_CONDOMLESS_ANAL_DATE,
       MAX(IF(exp_data.EXPOSURE = 'CONDOMLESS_VAGINAL', exp_data.IS_EXPOSED,
              NULL))                                                                                    AS RISK_CONDOMLESS_VAGINAL,
       MAX(IF(exp_data.EXPOSURE = 'CONDOMLESS_VAGINAL', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_CONDOMLESS_VAGINAL_DATE,

       -- sex in general
       MAX(IF(exp_data.EXPOSURE = 'SEX_EVER', exp_data.IS_EXPOSED, NULL))                               AS RISK_SEX_EVER,
       MAX(IF(exp_data.EXPOSURE = 'SEX_EVER', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_SEX_EVER_DATE,
       MAX(IF(exp_data.EXPOSURE = 'SEX_ORAL_ANAL', exp_data.IS_EXPOSED, NULL))                          AS RISK_SEX_ORAL_ANAL,
       MAX(IF(exp_data.EXPOSURE = 'SEX_ORAL_ANAL', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_SEX_ORAL_ANAL_DATE,

       -- transactional sex
       MAX(IF(exp_data.EXPOSURE = 'SEX_PAYING', exp_data.IS_EXPOSED, NULL))                             AS RISK_SEX_PAYING,
       MAX(IF(exp_data.EXPOSURE = 'SEX_PAYING', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_SEX_PAYING_DATE,
       MAX(IF(exp_data.EXPOSURE = 'SEX_PAYMENT', exp_data.IS_EXPOSED, NULL))                            AS RISK_SEX_PAYMENT,
       MAX(IF(exp_data.EXPOSURE = 'SEX_PAYMENT', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_SEX_PAYMENT_DATE,
       MAX(IF(exp_data.EXPOSURE = 'SEX_TRANSACT', exp_data.IS_EXPOSED, NULL))                           AS RISK_SEX_TRANSACT,
       MAX(IF(exp_data.EXPOSURE = 'SEX_TRANSACT', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_SEX_TRANSACT_DATE,

       -- hiv exposure
       MAX(IF(exp_data.EXPOSURE = 'SEX_HIV', exp_data.IS_EXPOSED, NULL))                                AS RISK_SEX_HIV,
       MAX(IF(exp_data.EXPOSURE = 'SEX_HIV', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_SEX_HIV_DATE,
       MAX(IF(exp_data.EXPOSURE = 'HIV_UNKNOWN', exp_data.IS_EXPOSED, NULL))                            AS RISK_HIV_UNKNOWN,
       MAX(IF(exp_data.EXPOSURE = 'HIV_UNKNOWN', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_HIV_UNKNOWN_DATE,
       MAX(IF(exp_data.EXPOSURE = 'HIV_VL_UNKNOWN', exp_data.IS_EXPOSED, NULL))                         AS RISK_HIV_VL_UNKNOWN,
       MAX(IF(exp_data.EXPOSURE = 'HIV_VL_UNKNOWN', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_HIV_VL_UNKNOWN_DATE,

       -- drugs exposure
       MAX(IF(exp_data.EXPOSURE = 'SEX_DRUGS', exp_data.IS_EXPOSED, NULL))                              AS RISK_SEX_DRUGS,
       MAX(IF(exp_data.EXPOSURE = 'SEX_DRUGS', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_SEX_DRUGS_DATE,
       MAX(IF(exp_data.EXPOSURE = 'DRUG_INJECT', exp_data.IS_EXPOSED, NULL))                            AS RISK_DRUG_INJECT,
       MAX(IF(exp_data.EXPOSURE = 'DRUG_INJECT', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_DRUG_INJECT_DATE,
       MAX(IF(exp_data.EXPOSURE = 'ILLICIT_DRUGS', exp_data.IS_EXPOSED, NULL))                          AS RISK_ILLICIT_DRUGS,
       MAX(IF(exp_data.EXPOSURE = 'ILLICIT_DRUGS', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_ILLICIT_DRUGS_DATE,

       -- others
       MAX(IF(exp_data.EXPOSURE = 'BLOOD_TRANSFUSE', exp_data.IS_EXPOSED, NULL))                        AS RISK_BLOOD_TRANSFUSE,
       MAX(IF(exp_data.EXPOSURE = 'BLOOD_TRANSFUSE', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_BLOOD_TRANSFUSE_DATE,
       MAX(IF(exp_data.EXPOSURE = 'OCCUPATION', exp_data.IS_EXPOSED, NULL))                             AS RISK_OCCUPATION,
       MAX(IF(exp_data.EXPOSURE = 'OCCUPATION', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_OCCUPATION_DATE,
       MAX(IF(exp_data.EXPOSURE = 'PEP', exp_data.IS_EXPOSED, NULL))                                    AS RISK_PEP,
       MAX(IF(exp_data.EXPOSURE = 'PEP', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_PEP_DATE,
       MAX(IF(exp_data.EXPOSURE = 'STI', exp_data.IS_EXPOSED, NULL))                                    AS RISK_STI,
       MAX(IF(exp_data.EXPOSURE = 'STI', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_STI_DATE,
       MAX(IF(exp_data.EXPOSURE = 'TATTOO', exp_data.IS_EXPOSED, NULL))                                 AS RISK_TATTOO,
       MAX(IF(exp_data.EXPOSURE = 'TATTOO', DATE(exp_data.DATE_LAST_EXPOSE),
              NULL))                                                                                    AS RISK_TATTOO_DATE
FROM (SELECT rec.REC_ID,
             rec.CREATED_AT,
             rec.UPDATED_AT,
             rec.DELETED_AT,
             CASE exp.EXPOSURE
                 WHEN '120000' THEN 'HIV_MOTHER'
                 WHEN '217000' THEN 'SEX_M'
                 WHEN '216000' THEN 'SEX_M_AV'
                 WHEN '216200' THEN 'SEX_M_AV_NOCONDOM'
                 WHEN '227000' THEN 'SEX_F'
                 WHEN '226000' THEN 'SEX_F_AV'
                 WHEN '226200' THEN 'SEX_F_AV_NOCONDOM'
                 WHEN '200010' THEN 'SEX_PAYING'
                 WHEN '200020' THEN 'SEX_PAYMENT'
                 WHEN '200300' THEN 'SEX_DRUGS'
                 WHEN '330000' THEN 'SEX_DRUGS'
                 WHEN '301010' THEN 'DRUG_INJECT'
                 WHEN '301200' THEN 'DRUG_INJECT'
                 WHEN '311010' THEN 'DRUG_INJECT'
                 WHEN '312000' THEN 'DRUG_INJECT'
                 WHEN '310000' THEN 'ILLICIT_DRUGS'
                 WHEN '530000' THEN 'BLOOD_TRANSFUSE'
                 WHEN '510000' THEN 'OCCUPATION'
                 WHEN '520000' THEN 'TATTOO'
                 WHEN '400000' THEN 'STI'
                 WHEN '200000' THEN 'SEX_EVER'
                 WHEN '210000' THEN 'SEX_ORAL_ANAL'
                 WHEN '200030' THEN 'SEX_TRANSACT'
                 WHEN '200001' THEN 'HIV_VL_UNKNOWN'
                 WHEN '230000' THEN 'HIV_UNKNOWN'
                 WHEN '320002' THEN 'PEP'
                 WHEN '201200' THEN 'CONDOMLESS_ANAL'
                 WHEN '202200' THEN 'CONDOMLESS_VAGINAL'
                 WHEN '261200' THEN 'CONDOMLESS_ANAL'
                 WHEN '262200' THEN 'CONDOMLESS_VAGINAL'
                 WHEN '210200' THEN 'SEX_M_NOCONDOM'
                 WHEN '220200' THEN 'SEX_F_NOCONDOM'
                 WHEN '230003' THEN 'SEX_HIV'
                 ELSE exp.EXPOSURE END AS EXPOSURE,
             CASE
                 WHEN exp.IS_EXPOSED = 1 AND exp.TYPE_LAST_EXPOSE = 4 THEN '1_Yes, within the past 12 months'
                 WHEN exp.IS_EXPOSED = 1 AND exp.TYPE_LAST_EXPOSE = 0 THEN '2_Yes'
                 WHEN exp.IS_EXPOSED = 1 AND exp.TYPE_LAST_EXPOSE IS NULL THEN '2_Yes'
                 WHEN exp.IS_EXPOSED = 1 AND exp.TYPE_LAST_EXPOSE = 3 THEN '3_Yes, within the past 6 months'
                 WHEN exp.IS_EXPOSED = 1 AND exp.TYPE_LAST_EXPOSE = 1 THEN '4_Yes, within the past 30 days'
                 WHEN exp.IS_EXPOSED = 0 THEN '0_No'
                 WHEN exp.IS_EXPOSED = 99999 THEN NULL
                 END                   AS IS_EXPOSED,
             DATE_LAST_EXPOSE
      FROM ohasis_interim.px_record AS rec
               JOIN ohasis_interim.px_expose_hist AS exp ON rec.REC_ID = exp.REC_ID
      WHERE exp.EXPOSURE <> '9999'
        AND ((rec.CREATED_AT BETWEEN ? AND ?) OR
             (rec.UPDATED_AT BETWEEN ? AND ?) OR
             (rec.DELETED_AT BETWEEN ? AND ?))) AS exp_data
GROUP BY exp_data.REC_ID;
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
