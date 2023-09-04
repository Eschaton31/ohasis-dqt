SELECT conf.REC_ID, conf.CONFIRM_CODE, test.DATE_RECEIVE, test.DATE_COLLECT
FROM ohasis_interim.px_confirm AS conf
         LEFT JOIN ohasis_interim.px_test_hiv AS test ON conf.REC_ID = test.REC_ID AND test.TEST_TYPE = 31
         LEFT JOIN ohasis_interim.px_record AS rec ON conf.REC_ID = rec.REC_ID
WHERE CONFIRM_CODE REGEXP '--'
  AND rec.DELETED_AT IS NULL;