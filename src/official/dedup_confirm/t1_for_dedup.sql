SELECT conf.REC_ID,
       r.PATIENT_ID,
       conf.CONFIRM_CODE,
       i.UIC,
       i.BIRTHDATE,
       i.SEX,
       i.PHILHEALTH_NO,
       n.FIRST,
       n.MIDDLE,
       n.LAST,
       n.SUFFIX,
       conf.FACI_ID,
       conf.SUB_FACI_ID,
       conf.SOURCE,
       conf.SUB_SOURCE,
       conf.FINAL_RESULT,
       conf.REMARKS
FROM ohasis_interim.px_confirm AS conf
         JOIN ohasis_interim.px_name AS n ON conf.REC_ID = n.REC_ID
         JOIN ohasis_interim.px_info AS i ON conf.REC_ID = i.REC_ID
         JOIN ohasis_interim.px_record AS r ON conf.REC_ID = r.REC_ID
WHERE r.DELETED_AT IS NULL
  AND conf.FINAL_RESULT REGEXP 'Pending';