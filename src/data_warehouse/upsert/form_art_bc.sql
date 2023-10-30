SELECT pii.REC_ID,
       pii.PATIENT_ID,
       pii.FORM_VERSION,
       pii.CREATED_BY,
       pii.CREATED_AT,
       pii.UPDATED_BY,
       pii.UPDATED_AT,
       pii.DELETED_BY,
       pii.DELETED_AT,
       pii.SNAPSHOT,
       pii.FACI_ID,
       pii.SUB_FACI_ID,
       CASE
           WHEN pii.RECORD_DATE = DATE(meds.DISP_DATE) THEN pii.RECORD_DATE
           WHEN pii.RECORD_DATE != DATE(meds.DISP_DATE) AND DATE(meds.DISP_DATE) != '0000-00-00'
               THEN DATE(meds.DISP_DATE)
           WHEN pii.RECORD_DATE IS NULL AND meds.DISP_DATE IS NOT NULL THEN DATE(meds.DISP_DATE)
           ELSE pii.RECORD_DATE
           END                             AS VISIT_DATE,
       pii.RECORD_DATE,
       pii.DISEASE,
       pii.MODULE,
       pii.PRIME,
       pii.CONFIRMATORY_CODE,
       pii.UIC,
       pii.PHILHEALTH_NO,
       pii.SEX,
       pii.BIRTHDATE,
       pii.PATIENT_CODE,
       pii.PHILSYS_ID,
       pii.FIRST,
       pii.MIDDLE,
       pii.LAST,
       pii.SUFFIX,
       pii.CLIENT_MOBILE,
       pii.CLIENT_EMAIL,
       pii.AGE,
       pii.AGE_MO,
       pii.GENDER_AFFIRM_THERAPY,
       pii.SELF_IDENT,
       pii.SELF_IDENT_OTHER,
       pii.NATIONALITY,
       pii.NATIONALITY_OTHER,
       pii.EDUC_LEVEL,
       pii.CIVIL_STATUS,
       pii.LIVING_WITH_PARTNER,
       pii.CHILDREN,
       pii.CURR_PSGC_REG,
       pii.CURR_PSGC_PROV,
       pii.CURR_PSGC_MUNC,
       pii.CURR_ADDR,
       service.MODALITY,
       service.SERVICE_FACI,
       service.SERVICE_SUB_FACI,
       service.REFER_FACI,
       service.SERVICE_BY,
       service.TX_STATUS,
       service.VISIT_TYPE,
       service.CLIENT_TYPE,
       service.CLINIC_NOTES,
       service.COUNSEL_NOTES,
       kp.KP_PDL,
       kp.KP_SW,
       kp.KP_TG,
       kp.KP_PWID,
       kp.KP_MSM,
       kp.KP_OFW,
       kp.KP_PARTNER,
       kp.KP_OTHER,
       staging.WHO_CLASS,
       lab.LAB_HEMOG_DATE,
       lab.LAB_HEMOG_RESULT,
       lab.LAB_VIRAL_DATE,
       lab.LAB_VIRAL_RESULT,
       lab.LAB_CD4_DATE,
       lab.LAB_CD4_RESULT,
       lab.LAB_CREA_DATE,
       lab.LAB_CREA_RESULT,
       lab.LAB_CREA_CLEARANCE,
       lab.LAB_HBSAG_DATE,
       lab.LAB_HBSAG_RESULT,
       lab.LAB_XRAY_DATE,
       lab.LAB_XRAY_RESULT,
       lab.LAB_XPERT_DATE,
       lab.LAB_XPERT_RESULT,
       lab.LAB_DSSM_DATE,
       lab.LAB_DSSM_RESULT,
       lab.LAB_HIVDR_DATE,
       lab.LAB_HIVDR_RESULT,
       vax.VAX_COVID19_1ST_DONE,
       vax.VAX_COVID19_1ST_VAX_USED,
       vax.VAX_COVID19_1ST_DATE,
       vax.VAX_COVID19_1ST_LOCATION,
       vax.VAX_COVID19_2ND_DONE,
       vax.VAX_COVID19_2ND_VAX_USED,
       vax.VAX_COVID19_2ND_DATE,
       vax.VAX_COVID19_2ND_LOCATION,
       vax.VAX_COVID19_BOOST_DONE,
       vax.VAX_COVID19_BOOST_VAX_USED,
       vax.VAX_COVID19_BOOST_DATE,
       vax.VAX_COVID19_BOOST_LOCATION,
       vax.VAX_HEPB_1ST_DONE,
       vax.VAX_HEPB_1ST_VAX_USED,
       vax.VAX_HEPB_1ST_DATE,
       vax.VAX_HEPB_1ST_LOCATION,
       vax.VAX_HEPB_2ND_DONE,
       vax.VAX_HEPB_2ND_VAX_USED,
       vax.VAX_HEPB_2ND_DATE,
       vax.VAX_HEPB_2ND_LOCATION,
       vax.VAX_HEPB_3RD_DONE,
       vax.VAX_HEPB_3RD_VAX_USED,
       vax.VAX_HEPB_3RD_DATE,
       vax.VAX_HEPB_3RD_LOCATION,
       tb.TB_SCREEN,
       tb.TB_STATUS,
       tb.TB_ACTIVE_ALREADY,
       tb.TB_TX_ALREADY,
       tb.TB_SITE_P,
       tb.TB_SITE_EP,
       tb.TB_DRUG_RESISTANCE,
       tb.TB_DRUG_RESISTANCE_OTHER,
       tb.TB_TX_STATUS,
       tb.TB_TX_STATUS_OTHER,
       tb.TB_REGIMEN,
       tb.TB_TX_START_DATE,
       tb.TB_TX_END_DATE,
       tb.TB_TX_OUTCOME,
       tb.TB_TX_OUTCOME_OTHER,
       tb.TB_IPT_STATUS,
       tb.TB_IPT_START_DATE,
       tb.TB_IPT_END_DATE,
       tb.TB_IPT_OUTCOME,
       tb.TB_IPT_OUTCOME_OTHER,
       proph.PROPH_COTRI_DONE              AS PROPH_COTRI,
       proph.PROPH_AZITHRO_DONE            AS PROPH_AZITHRO,
       proph.PROPH_FLUCANO_DONE            AS PROPH_FLUCANO,
       oi.OI_SYPH                          AS OI_SYPH_PRESENT,
       oi.OI_HEPB                          AS OI_HEPB_PRESENT,
       oi.OI_HEPC                          AS OI_HEPC_PRESENT,
       oi.OI_PCP                           AS OI_PCP_PRESENT,
       oi.OI_CMV                           AS OI_CMV_PRESENT,
       oi.OI_OROCAND                       AS OI_OROCAND_PRESENT,
       oi.OI_HERPES                        AS OI_HERPES_PRESENT,
       oi.OI_OTHER                         AS OI_OTHER_PRESENT,
       oi.OI_OTHER_TEXT                    AS OI_OTHER_TEXT,
       ob.IS_PREGNANT,
       ob.LMP,
       ob.PREG_WKS                         AS AOG,
       ob.EDD,
       ob.DELIVER_DATE,
       ob.DELIVER_FACI,
       ob.FEED_TYPE,
       meds.REC_ID_GRP,
       meds.FACI_DISP,
       meds.SUB_FACI_DISP,
       meds.MEDICINE_SUMMARY,
       meds.DISP_DATE,
       meds.LATEST_NEXT_DATE,
       COALESCE(ROUND((LENGTH(meds.MEDICINE_SUMMARY) - LENGTH(REPLACE(meds.MEDICINE_SUMMARY, '+', ''))) /
                      LENGTH('+')) + 1, 0) AS NUM_OF_DRUGS,
       CASE
           WHEN MEDICINE_SUMMARY IS NOT NULL THEN 'ART'
           WHEN LEFT(TX_STATUS, 1) IN ('1', '2') THEN 'ART'
           WHEN LEFT(TX_STATUS, 1) = '0' THEN 'Care'
           ELSE 'Care'
           END                             AS ART_RECORD
FROM ohasis_lake.px_pii AS pii
         LEFT JOIN ohasis_lake.px_faci_info AS service ON pii.REC_ID = service.REC_ID
         LEFT JOIN ohasis_lake.px_key_pop AS kp ON pii.REC_ID = kp.REC_ID
         LEFT JOIN ohasis_lake.px_staging AS staging ON pii.REC_ID = staging.REC_ID
         LEFT JOIN ohasis_lake.lab_wide AS lab ON pii.REC_ID = lab.REC_ID
         LEFT JOIN ohasis_lake.px_vaccine AS vax ON pii.REC_ID = vax.REC_ID
         LEFT JOIN ohasis_lake.px_tb_info AS tb ON pii.REC_ID = tb.REC_ID
         LEFT JOIN ohasis_lake.px_prophylaxis AS proph ON pii.REC_ID = proph.REC_ID
         LEFT JOIN ohasis_lake.px_oi AS oi ON pii.REC_ID = oi.REC_ID
         LEFT JOIN ohasis_lake.px_ob AS ob ON pii.REC_ID = ob.REC_ID
         LEFT JOIN (SELECT disp.REC_ID,
                           disp.REC_ID_GRP,
                           disp.FACI_ID                                                     AS FACI_DISP,
                           disp.SUB_FACI_ID                                                 AS SUB_FACI_DISP,
                           GROUP_CONCAT(DISTINCT meds.SHORT ORDER BY `ORDER` SEPARATOR '+') AS MEDICINE_SUMMARY,
                           MAX(DISP_DATE)                                                   AS DISP_DATE,
                           MAX(NEXT_DATE)                                                   AS LATEST_NEXT_DATE
                    FROM ohasis_lake.disp_meds AS disp
                             LEFT JOIN ohasis_lake.disc_meds AS disc
                                       ON disp.REC_ID = disc.REC_ID AND disp.MEDICINE = disc.MEDICINE
                             JOIN ohasis_lake.ref_items AS meds ON disp.MEDICINE = meds.ITEM
                    WHERE disc.DISC_REASON IS NULL
                       OR disc.DISC_REASON_OTHER REGEXP 'NEGATIVE CONFIRMATORY'
                    GROUP BY disp.REC_ID, disp.REC_ID_GRP) AS meds
                   ON pii.REC_ID = meds.REC_ID
WHERE pii.DISEASE = 'HIV'
  AND MODULE = '3_Treatment'
  AND ((pii.CREATED_AT BETWEEN ? AND ?) OR
       (pii.UPDATED_AT BETWEEN ? AND ?) OR
       (pii.DELETED_AT BETWEEN ? AND ?));;
-- ID_COLS: REC_ID, REC_ID_GRP;
-- DELETE: DELETED_AT IS NOT NULL OR (FORM_VERSION NOT LIKE '%ART%' AND FORM_VERSION NOT LIKE '%BC%');
