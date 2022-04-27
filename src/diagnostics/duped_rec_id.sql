SELECT r.REC_ID,
       r.PATIENT_ID,
       r.FACI_ID,
       r.RECORD_DATE,
       n.FIRST,
       n.MIDDLE,
       n.LAST,
       n.SUFFIX,
       i.CONFIRMATORY_CODE,
       i.PATIENT_CODE,
       i.UIC,
       i.BIRTHDATE,
       i.SEX
FROM ohasis_interim.px_record AS r
         LEFT JOIN ohasis_interim.px_name AS n ON r.REC_ID = n.REC_ID AND r.PATIENT_ID = n.PATIENT_ID
         LEFT JOIN ohasis_interim.px_info AS i ON r.REC_ID = i.REC_ID AND r.PATIENT_ID = i.PATIENT_ID
WHERE r.REC_ID IN (SELECT REC_ID FROM ohasis_interim.px_record GROUP BY REC_ID HAVING COUNT(*) > 1)