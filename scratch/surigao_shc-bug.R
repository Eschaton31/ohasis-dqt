affected <- QB$new(`oh-live`)$
   select("rec.REC_ID")$
   from("ohasis_interim.px_record AS rec")$
   join("ohasis_interim.px_faci AS service", "rec.REC_ID", "=", "service.REC_ID")$
   where("rec.FACI_ID", "<>", "service.FACI_ID")$
   where("service.FACI_ID", "=", "040002")$
   # where("service.FACI_ID", "=", "160165")$
   get()

affected <- QB$new(`oh-live`)$
   select("rec.REC_ID")$
   from("ohasis_interim.px_record AS rec")$
   join("ohasis_interim.px_test AS service", "rec.REC_ID", "=", "service.REC_ID")$
   where("rec.FACI_ID", "<>", "service.FACI_ID")$
   where("service.FACI_ID", "=", "040002")$
   # where("service.FACI_ID", "=", "160165")$
   distinct()$
   get()

live <- list(
   px_record = QB$new(`oh-live`)$from("ohasis_interim.px_record")$whereIn("REC_ID", affected$REC_ID)$get(),
   px_faci   = QB$new(`oh-live`)$from("ohasis_interim.px_faci")$whereIn("REC_ID", affected$REC_ID)$get(),
   px_test   = QB$new(`oh-live`)$from("ohasis_interim.px_test")$whereIn("REC_ID", affected$REC_ID)$where("TEST_TYPE", "10")$get()
)

lake <- list(
   px_faci_info   = QB$new(`oh-lw`)$from("ohasis_lake.px_faci_info")$whereIn("REC_ID", affected$REC_ID)$get(),
   px_hiv_testing = QB$new(`oh-lw`)$from("ohasis_lake.px_hiv_testing")$whereIn("REC_ID", affected$REC_ID)$get()
)


px_faci_issue <- lake$px_faci_info %>%
   select(
      REC_ID,
      SERVICE_TYPE = MODALITY,
      FACI_ID      = SERVICE_FACI,
      SUB_FACI_ID  = SERVICE_SUB_FACI,
      PROVIDER_ID = SERVICE_BY
   ) %>%
   mutate(SERVICE_TYPE = str_left(SERVICE_TYPE, 6)) %>%
   anti_join(
      y = live$px_faci %>%
         select(
            REC_ID,
            SERVICE_TYPE,
            FACI_ID,
            SUB_FACI_ID,
            PROVIDER_ID
         )
   )

px_test_issue <- lake$px_faci_info %>%
   select(
      REC_ID,
      FACI_ID     = SERVICE_FACI,
      SUB_FACI_ID = SERVICE_SUB_FACI,
   ) %>%
   inner_join(lake$px_hiv_testing %>% distinct(REC_ID)) %>%
   anti_join(
      y = live$px_test %>%
         select(
            REC_ID,
            FACI_ID,
            SUB_FACI_ID,
            TEST_TYPE,
            TEST_NUM
         )
   ) %>%
   inner_join(
      y  = live$px_test %>%
         select(
            REC_ID,
            TEST_TYPE,
            TEST_NUM,
            DATE_PERFORM,
            RESULT
         ),
      by = join_by(REC_ID)
   )

px_test_issue <- live$px_test %>%
   filter(TEST_TYPE == "10", FACI_ID == "040002") %>%
   # filter(TEST_TYPE == "10", FACI_ID == "160165") %>%
   select(
      REC_ID,
      TEST_TYPE,
      TEST_NUM,
   ) %>%
   inner_join(
      y  = lake$px_hiv_testing %>%
         mutate(
            T0_RESULT = keep_code(coalesce(T0_RESULT, T1_RESULT))
         ) %>%
         select(
            REC_ID,
            DATE_PERFORM = T0_DATE,
            RESULT       = T0_RESULT
         ),
      by = join_by(REC_ID)
   ) %>%
   inner_join(
      y  = lake$px_faci_info %>%
         select(
            REC_ID,
            FACI_ID     = SERVICE_FACI,
            SUB_FACI_ID = SERVICE_SUB_FACI,
         ),
      by = join_by(REC_ID)
   # ) %>%
   # inner_join(
   #    y  = live$px_record %>%
   #       select(
   #          REC_ID,
   #          FACI_ID     ,
   #          SUB_FACI_ID,
   #       ),
   #    by = join_by(REC_ID)
   )

# px_faci_issue <- live$px_faci %>%
#    select(
#       REC_ID,
#       SERVICE_TYPE,
#       SERVICE_FACI = FACI_ID
#    ) %>%
#    left_join(
#       y = live$px_record %>%
#          select(
#             REC_ID,
#             FACI_ID,
#             SUB_FACI_ID,
#          )
#    ) %>%
#    filter(SERVICE_FACI != FACI_ID) %>%
#    select(-SERVICE_FACI)
#
#
# px_test_issue <- live$px_test %>%
#    select(
#       REC_ID,
#       TEST_TYPE,
#       TEST_NUM,
#       SERVICE_FACI = FACI_ID
#    ) %>%
#    left_join(
#       y = live$px_record %>%
#          select(
#             REC_ID,
#             FACI_ID,
#             SUB_FACI_ID,
#          )
#    ) %>%
#    filter(SERVICE_FACI != FACI_ID) %>%
#    select(-SERVICE_FACI)


con <- connect("ohasis-live")
dbxUpsert(con, Id(schema = "ohasis_interim", table = "px_faci"), px_faci_issue, c("REC_ID", "SERVICE_TYPE"))
dbxUpsert(con, Id(schema = "ohasis_interim", table = "px_test"), px_test_issue, c("REC_ID", "TEST_TYPE", "TEST_NUM"))
dbDisconnect(con)
