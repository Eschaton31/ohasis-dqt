select rec.REC_ID,
       coalesce(reg.CENTRAL_ID, rec.PATIENT_ID)                                                        as CENTRAL_ID,
       rec.PATIENT_ID,
       rec.CREATED_BY,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.UPDATED_BY,
       rec.RECORD_DATE                                                                                 AS VISIT_DATE,
       greatest(coalesce(rec.DELETED_AT, 0), coalesce(rec.UPDATED_AT, 0), coalesce(rec.CREATED_AT, 0)) as SNAPSHOT,
       conf.FACI_ID                                                                                    as CONFIRM_FACI,
       conf.SUB_FACI_ID                                                                                as CONFIRM_SUB_FACI,
       case conf.CONFIRM_TYPE
           when 1 then '1_Central NRL'
           when 2 then '2_CrCL'
           else conf.CONFIRM_TYPE end                                                                  as CONFIRM_TYPE,
       conf.CONFIRM_CODE,
       case
           when (info.SEX = 1) then 'MALE'
           when (info.SEX = 2) then 'FEMALE'
           end                                                                                         as SEX,
       info.BIRTHDATE,
       coalesce(profile.AGE, TIMESTAMPDIFF(YEAR, info.BIRTHDATE, rec.RECORD_DATE))                     as AGE,
       case conf.CLIENT_TYPE
           when '1' then '1_Inpatient'
           when '2' then '2_Walk-in / Outpatient'
           when '3' then '3_Mobile HTS Client'
           when '5' then '5_Satellite Client'
           when '4' then '4_Referral'
           when '6' then '6_Transient'
           else conf.CLIENT_TYPE end                                                                   as SPECIMEN_REFER_TYPE,
       conf.SOURCE                                                                                     as SPECIMEN_SOURCE,
       conf.SUB_SOURCE                                                                                 as SPECIMEN_SUB_SOURCE,
       case
           when conf.FINAL_RESULT like '%Positive%' then '1_Positive'
           when conf.FINAL_RESULT like '%Negative%' then '2_Negative'
           when conf.FINAL_RESULT like '%Inconclusive%' then '3_Indeterminate'
           when conf.FINAL_RESULT like '%Indeterminate%' then '3_Indeterminate'
           when conf.FINAL_RESULT like '%PENDING%' then '4_Pending'
           when conf.FINAL_RESULT like '%Duplicate%' then '5_Duplicate'
           else conf.FINAL_RESULT end                                                                  as CONFIRM_RESULT,
       conf.SIGNATORY_1,
       conf.SIGNATORY_2,
       conf.SIGNATORY_3,
       conf.DATE_RELEASE,
       conf.DATE_CONFIRM,
       conf.IDNUM,
       case rtri.RT_AGREED
           when 1 then '1_Yes'
           when 0 then '0_No'
           else RT_AGREED end                                                                          as RT_AGREED,
       max(IF(test_hiv.TEST_TYPE = 60, test.DATE_PERFORM, null))                                       as RT_DATE,
       max(case
               when test_hiv.TEST_TYPE = 60 and test_hiv.KIT_NAME = '1014' then 'Asante HIV-1 Rapid Recency Assay'
               when test_hiv.TEST_TYPE = 60 and test_hiv.KIT_NAME is not null then test_hiv.KIT_NAME
           end)                                                                                        as RT_KIT,
       case
           when rtri.RT_RESULT like 'Recent%' then '1_Recent'
           when rtri.RT_RESULT like 'Long%' then '2_Long-term'
           when rtri.RT_RESULT like 'Inconclusive%' then '3_Inconclusive'
           when rtri.RT_RESULT like 'Invalid%' then '0_Invalid'
           end                                                                                         as RT_RESULT,
       case rtri.VL_REQUESTED
           when 1 then '1_Yes'
           when 0 then '0_No'
           else VL_REQUESTED end                                                                       as RT_VL_REQUESTED,
       case rtri.VL_DONE
           when 1 then '1_Yes'
           when 0 then '0_No'
           else VL_DONE end                                                                            as RT_VL_DONE,
       labs.LAB_DATE                                                                                   as RT_VL_DATE,
       labs.LAB_RESULT                                                                                 as RT_VL_RESULT,
       max(IF(test.TEST_TYPE = 10, test.DATE_PERFORM, null))                                           as T0_DATE,
       max(case
               when test.TEST_TYPE = 10 and test.RESULT = 1 then '1_Reactive'
               when test.TEST_TYPE = 10 and test.RESULT = 2 then '2_Non-reactive'
               else null end)                                                                          as T0_RESULT,
       max(IF(test_hiv.TEST_TYPE = 31, test.DATE_PERFORM, null))                                       as T1_DATE,
       max(IF(test_hiv.TEST_TYPE = 31, test_hiv.KIT_NAME, null))                                       as T1_KIT,
       max(case
               when test_hiv.TEST_TYPE = 31 and test_hiv.FINAL_RESULT like '1%' then '1_Positive / Reactive'
               when test_hiv.TEST_TYPE = 31 and test_hiv.FINAL_RESULT like '2%' then '2_Negative / Non-reactive'
               when test_hiv.TEST_TYPE = 31 and test_hiv.FINAL_RESULT like '3%' then '3_Indeterminate / Inconclusive'
               when test_hiv.TEST_TYPE = 31 and test_hiv.FINAL_RESULT like '0%' then '0_Invalid'
               else null end)                                                                          as T1_RESULT,
       max(IF(test_hiv.TEST_TYPE = 32, test.DATE_PERFORM, null))                                       as T2_DATE,
       max(IF(test_hiv.TEST_TYPE = 32, test_hiv.KIT_NAME, null))                                       as T2_KIT,
       max(case
               when test_hiv.TEST_TYPE = 32 and test_hiv.FINAL_RESULT like '1%' then '1_Positive / Reactive'
               when test_hiv.TEST_TYPE = 32 and test_hiv.FINAL_RESULT like '2%' then '2_Negative / Non-reactive'
               when test_hiv.TEST_TYPE = 32 and test_hiv.FINAL_RESULT like '3%' then '3_Indeterminate / Inconclusive'
               when test_hiv.TEST_TYPE = 32 and test_hiv.FINAL_RESULT like '0%' then '0_Invalid'
               else null end)                                                                          as T2_RESULT,
       max(IF(test_hiv.TEST_TYPE = 33, test.DATE_PERFORM, null))                                       as T3_DATE,
       max(IF(test_hiv.TEST_TYPE = 33, test_hiv.KIT_NAME, null))                                       as T3_KIT,
       max(case
               when test_hiv.TEST_TYPE = 33 and test_hiv.FINAL_RESULT like '1%' then '1_Positive / Reactive'
               when test_hiv.TEST_TYPE = 33 and test_hiv.FINAL_RESULT like '2%' then '2_Negative / Non-reactive'
               when test_hiv.TEST_TYPE = 33 and test_hiv.FINAL_RESULT like '3%' then '3_Indeterminate / Inconclusive'
               when test_hiv.TEST_TYPE = 33 and test_hiv.FINAL_RESULT like '0%' then '0_Invalid'
               else null end)                                                                          as T3_RESULT
from ohasis_interim.px_record as rec
         left join ohasis_interim.px_test as test on rec.REC_ID = test.REC_ID
         left join ohasis_interim.px_info as info on rec.REC_ID = info.REC_ID
         left join ohasis_interim.px_profile as profile on rec.REC_ID = profile.REC_ID
         left join ohasis_interim.px_confirm as conf on rec.REC_ID = conf.REC_ID
         left join ohasis_interim.px_rtri as rtri on rec.REC_ID = rtri.REC_ID
         left join ohasis_interim.px_labs as labs on rec.REC_ID = labs.REC_ID and labs.LAB_TEST = 4
         left join ohasis_interim.px_test_hiv as test_hiv
                   on test.REC_ID = test_hiv.REC_ID and test.TEST_TYPE = test_hiv.TEST_TYPE and
                      test.TEST_NUM = test_hiv.TEST_NUM
         left join ohasis_interim.registry as reg on rec.PATIENT_ID = reg.PATIENT_ID
where (conf.SOURCE in
       '070010', '070078', '070013', '070003', '070004', '070002', '070019', '070009', '070008', '070045', '070108',
       '070111', '060007', '060001', '060003', '060008', '060037', '060077', '060237', '060232', '060023', '060004',
       '060069', '060049', '130411', '130342', '130581', '130299', '130657', '130015', '130182', '130022', '130554'
       '130026')
   or conf.SOURCE is null)
  and (conf.FACI_ID in ('070010', '060007', '060001', '060008', '060003') OR
       (conf.FACI_ID = '130023' AND conf.SUB_FACI_ID = '130023_001'))
  and conf.FINAL_RESULT REGEXP 'Positive'
  and rec.RECORD_DATE >= '2023-03-01'
group by rec.REC_ID;