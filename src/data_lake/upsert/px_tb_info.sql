SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       CASE tb.TB_SCREEN
           WHEN 0 THEN '0_No'
           WHEN 1 THEN '1_Yes'
           END                                                                                         AS TB_SCREEN,
       CASE tb.TB_ACTIVE_ALREADY
           WHEN 0 THEN '0_No'
           WHEN 1 THEN '1_Yes'
           END                                                                                         AS TB_ACTIVE_ALREADY,
       CASE tb.TB_TX_ALREADY
           WHEN 0 THEN '0_No'
           WHEN 1 THEN '1_Yes'
           END                                                                                         AS TB_TX_ALREADY,
       CASE tb.TB_STATUS
           WHEN 0 THEN '0_No active TB'
           WHEN 1 THEN '1_With active TB'
           END                                                                                         AS TB_STATUS,
       CASE tb_yes.TB_SITE_P
           WHEN 0 THEN '0_No'
           WHEN 1 THEN '1_Yes'
           END                                                                                         AS TB_SITE_P,
       CASE tb_yes.TB_SITE_EP
           WHEN 0 THEN '0_No'
           WHEN 1 THEN '1_Yes'
           END                                                                                         AS TB_SITE_EP,
       CASE tb_yes.TB_DRUG_RESISTANCE
           WHEN 1 THEN '1_Susceptible'
           WHEN 2 THEN '2_MDR'
           WHEN 3 THEN '3_XDR'
           WHEN 4 THEN '4_RR only'
           WHEN 8888 THEN '8888_Other'
           END                                                                                         AS TB_DRUG_RESISTANCE,
       tb_yes.TB_DRUG_RESISTANCE_OTHER,
       CASE tb_yes.TB_TX_STATUS
           WHEN 0 THEN '0_Not on Tx'
           WHEN 11 THEN '11_Ongoing Tx'
           WHEN 12 THEN '12_Started Tx'
           WHEN 13 THEN '13_Ended Tx'
           END                                                                                         AS TB_TX_STATUS,
       tb_yes.TB_TX_STATUS_OTHER,
       CASE tb_yes.TB_REGIMEN
           WHEN 10 THEN '10_Cat I'
           WHEN 11 THEN '11_Cat Ia'
           WHEN 20 THEN '20_Cat II'
           WHEN 21 THEN '21_Cat IIa'
           WHEN 30 THEN '30_SRDR'
           WHEN 40 THEN '40_XDR-TB'
           END                                                                                         AS TB_REGIMEN,
       tb_yes.TB_TX_START_DATE,
       tb_yes.TB_TX_END_DATE,
       CASE tb_yes.TB_TX_OUTCOME
           WHEN 10 THEN '10_Not yet evaluated'
           WHEN 11 THEN '11_Cured'
           WHEN 20 THEN '20_Failed'
           WHEN 8888 THEN '8888_Other'
           END                                                                                         AS TB_TX_OUTCOME,
       tb_yes.TB_TX_OUTCOME_OTHER
FROM ohasis_interim.px_record AS rec
         JOIN ohasis_interim.px_tb AS tb ON rec.REC_ID = tb.REC_ID
         JOIN ohasis_interim.px_tb_active AS tb_yes ON rec.REC_ID = tb_yes.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
