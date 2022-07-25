##  Wide Tables ----------------------------------------------------------------

eharp$sql$wide <- list(
   "px_record"         = c("REC_ID", "PATIENT_ID"),
   "px_info"           = c("REC_ID", "PATIENT_ID"),
   "px_name"           = c("REC_ID", "PATIENT_ID"),
   "px_faci"           = c("REC_ID", "SERVICE_TYPE"),
   "px_profile"        = "REC_ID",
   "px_staging"        = "REC_ID",
   "px_ob"             = "REC_ID",
   "px_occupation"     = "REC_ID",
   "px_ofw"            = "REC_ID",
   "px_expose_profile" = "REC_ID",
   "px_prev_test"      = "REC_ID",
   "px_confirm"        = "REC_ID",
   "px_form"           = c("REC_ID", "FORM")
)

lapply(names(eharp$sql$wide), function(table) {
   db_conn <- ohasis$conn("db")
   cols    <- colnames(tbl(db_conn, dbplyr::in_schema("ohasis_interim", table)))

   col_select <- intersect(cols, names(.GlobalEnv$eharp$data$records))

   if (!(table %in% c("px_record", "px_confirm")))
      .GlobalEnv$eharp$tables[[table]] <- .GlobalEnv$eharp$data$records %>%
         select(-FACI_ID) %>%
         rename(FACI_ID = DX_FACI) %>%
         select(
            col_select
         )
   else
      .GlobalEnv$eharp$tables[[table]] <- .GlobalEnv$eharp$data$records %>%
         select(
            col_select
         )

   dbDisconnect(db_conn)
})


##  Long tables ----------------------------------------------------------------

eharp$sql$long <- list(
   "px_addr"        = c("REC_ID", "ADDR_TYPE"),
   "px_expose_hist" = c("REC_ID", "EXPOSURE"),
   "px_med_profile" = c("REC_ID", "PROFILE"),
   "px_test_reason" = c("REC_ID", "REASON"),
   "px_test"        = c("REC_ID", "TEST_TYPE", "TEST_NUM"),
   "px_test_hiv"    = c("REC_ID", "TEST_TYPE", "TEST_NUM")
   # "px_remarks"     = c("REC_ID", "REMARK_TYPE")
)

# px_faci
eharp$tables$px_faci <- eharp$tables$px_faci %>%
   bind_rows(
      eharp$data$records %>%
         select(
            REC_ID,
            FACI_ID,
            REFER_BY_ID = DX_FACI,
            CLIENT_TYPE,
            CREATED_BY,
            CREATED_AT,
            UPDATED_BY,
            UPDATED_AT,
         ) %>%
         mutate(SERVICE_TYPE = "101102")
   )

# addr
eharp$tables$px_addr <- eharp$data$records %>%
   select(
      EH_ID,
      RECORD_DATE,
      REC_ID,
      CREATED_AT,
      CREATED_BY,
   ) %>%
   inner_join(
      y  = eharp$exports$px_addr %>%
         select(
            EH_ID       = PATIENT_ID,
            RECORD_DATE = BEGDA,
            ADDR_TYPE,
            ADDR_REG,
            ADDR_PROV,
            ADDR_MUNC,
            ADDR_TEXT   = STREET
         ),
      by = c("EH_ID", "RECORD_DATE")
   ) %>%
   mutate(
      ADDR_TYPE = case_when(
         ADDR_TYPE == 1 ~ "PERM",
         ADDR_TYPE == 2 ~ "CURR",
         ADDR_TYPE == 5 ~ "BIRTH",
         TRUE ~ as.character(ADDR_TYPE)
      ),
      ADDR_TYPE = case_when(
         ADDR_TYPE == "PERM" ~ 2,
         ADDR_TYPE == "CURR" ~ 1,
         ADDR_TYPE == "BIRTH" ~ 3,
      ),
      ADDR_REG  = case_when(
         ADDR_PROV == "184500000" ~ "060000000",
         ADDR_PROV == "184600000" ~ "070000000",
         TRUE ~ ADDR_REG
      ),
      ADDR_PROV = case_when(
         ADDR_PROV == "184500000" ~ "064500000",
         ADDR_PROV == "184600000" ~ "074600000",
         TRUE ~ ADDR_PROV
      ),
      ADDR_MUNC = case_when(
         ADDR_MUNC == "184501000" ~ "064501000",
         ADDR_MUNC == "184502000" ~ "064502000",
         ADDR_MUNC == "184503000" ~ "064503000",
         ADDR_MUNC == "184504000" ~ "064504000",
         ADDR_MUNC == "184505000" ~ "064505000",
         ADDR_MUNC == "184506000" ~ "064506000",
         ADDR_MUNC == "184507000" ~ "064507000",
         ADDR_MUNC == "184508000" ~ "064508000",
         ADDR_MUNC == "184509000" ~ "064509000",
         ADDR_MUNC == "184510000" ~ "064510000",
         ADDR_MUNC == "184511000" ~ "064511000",
         ADDR_MUNC == "184512000" ~ "064512000",
         ADDR_MUNC == "184513000" ~ "064513000",
         ADDR_MUNC == "184514000" ~ "064514000",
         ADDR_MUNC == "184515000" ~ "064515000",
         ADDR_MUNC == "184516000" ~ "064516000",
         ADDR_MUNC == "184517000" ~ "064517000",
         ADDR_MUNC == "184518000" ~ "064518000",
         ADDR_MUNC == "184519000" ~ "064519000",
         ADDR_MUNC == "184520000" ~ "064520000",
         ADDR_MUNC == "184521000" ~ "064521000",
         ADDR_MUNC == "184522000" ~ "064522000",
         ADDR_MUNC == "184523000" ~ "064523000",
         ADDR_MUNC == "184524000" ~ "064524000",
         ADDR_MUNC == "184525000" ~ "064525000",
         ADDR_MUNC == "184526000" ~ "064526000",
         ADDR_MUNC == "184527000" ~ "064527000",
         ADDR_MUNC == "184528000" ~ "064528000",
         ADDR_MUNC == "184529000" ~ "064529000",
         ADDR_MUNC == "184530000" ~ "064530000",
         ADDR_MUNC == "184531000" ~ "064531000",
         ADDR_MUNC == "184532000" ~ "064532000",
         ADDR_MUNC == "184601000" ~ "074601000",
         ADDR_MUNC == "184602000" ~ "074602000",
         ADDR_MUNC == "184603000" ~ "074603000",
         ADDR_MUNC == "184604000" ~ "074604000",
         ADDR_MUNC == "184605000" ~ "074605000",
         ADDR_MUNC == "184606000" ~ "074606000",
         ADDR_MUNC == "184607000" ~ "074607000",
         ADDR_MUNC == "184608000" ~ "074608000",
         ADDR_MUNC == "184609000" ~ "074609000",
         ADDR_MUNC == "184610000" ~ "074610000",
         ADDR_MUNC == "184611000" ~ "074611000",
         ADDR_MUNC == "184612000" ~ "074612000",
         ADDR_MUNC == "184613000" ~ "074613000",
         ADDR_MUNC == "184614000" ~ "074614000",
         ADDR_MUNC == "184615000" ~ "074615000",
         ADDR_MUNC == "184616000" ~ "074616000",
         ADDR_MUNC == "184617000" ~ "074617000",
         ADDR_MUNC == "184618000" ~ "074618000",
         ADDR_MUNC == "184619000" ~ "074619000",
         ADDR_MUNC == "184620000" ~ "074620000",
         ADDR_MUNC == "184621000" ~ "074621000",
         ADDR_MUNC == "184622000" ~ "074622000",
         ADDR_MUNC == "184623000" ~ "074623000",
         ADDR_MUNC == "184624000" ~ "074624000",
         ADDR_MUNC == "184625000" ~ "074625000",
         TRUE ~ ADDR_MUNC
      )
   ) %>%
   select(-EH_ID, -RECORD_DATE)


# exposure
eharp$tables$px_expose_hist <- eharp$data$records %>%
   select(
      REC_ID,
      CREATED_AT,
      CREATED_BY,
      starts_with("EXPOSE")
   ) %>%
   pivot_longer(
      cols      = starts_with("EXPOSE"),
      names_to  = "EXPOSE_DATA",
      values_to = "EXPOSE_VALUE"
   ) %>%
   mutate(
      EXPOSE_DATA = if_else(
         condition = !stri_detect_regex(EXPOSE_DATA, "DATE$"),
         true      = paste0(EXPOSE_DATA, "_EXPOSED"),
         false     = EXPOSE_DATA,
         missing   = EXPOSE_DATA
      ),
      EXPOSURE    = substr(EXPOSE_DATA, 8, stri_locate_last_fixed(EXPOSE_DATA, "_") - 1),
      PIECE       = substr(EXPOSE_DATA, stri_locate_last_fixed(EXPOSE_DATA, "_") + 1, 1000),
   ) %>%
   mutate(
      EXPOSURE = case_when(
         EXPOSURE == "HIV_MOTHER" ~ "120000",
         EXPOSURE == "DRUG_INJECT" ~ "301010",
         EXPOSURE == "BLOOD_TRANSFUSION" ~ "530000",
         EXPOSURE == "OCCUPATION" ~ "510000",
         EXPOSURE == "SEX_DRUGS" ~ "200300",
         EXPOSURE == "SEX_M" ~ "217000",
         EXPOSURE == "SEX_M_AV" ~ "216000",
         EXPOSURE == "SEX_M_AV_NOCONDOM" ~ "216200",
         EXPOSURE == "SEX_F" ~ "227000",
         EXPOSURE == "SEX_F_AV" ~ "226000",
         EXPOSURE == "SEX_F_AV_NOCONDOM" ~ "226200",
         EXPOSURE == "SEX_PAYING" ~ "200010",
         EXPOSURE == "SEX_PAYMENT" ~ "200020",
         EXPOSURE == "SEX_M_NOCONDOM" ~ "210200",
         EXPOSURE == "SEX_F_NOCONDOM" ~ "220200",
         EXPOSURE == "STI" ~ "400000",
         EXPOSURE == "SEX_WITH_HIV" ~ "230003",
         EXPOSURE == "TATTOO" ~ "520000",
         TRUE ~ EXPOSURE
      )
   ) %>%
   pivot_wider(
      id_cols     = c(REC_ID, CREATED_AT, CREATED_BY, EXPOSURE),
      names_from  = PIECE,
      values_from = EXPOSE_VALUE
   ) %>%
   mutate(
      TYPE_LAST_EXPOSE = if_else(
         condition = EXPOSED == 2,
         true      = 4,
         false     = 0,
         missing   = 0
      ),
      EXPOSED          = if_else(
         condition = EXPOSED == 2,
         true      = "1",
         false     = as.character(EXPOSED),
         missing   = as.character(EXPOSED)
      ),
   ) %>%
   filter(!is.na(EXPOSED)) %>%
   rename(
      IS_EXPOSED = EXPOSED
   )

# med_profile
eharp$tables$px_med_profile <- eharp$data$records %>%
   select(
      REC_ID,
      CREATED_AT,
      CREATED_BY,
      starts_with("MED")
   ) %>%
   pivot_longer(
      cols      = starts_with("MED"),
      names_to  = "PROFILE",
      values_to = "IS_PROFILE"
   ) %>%
   mutate(
      PROFILE = stri_replace_all_regex(PROFILE, "^MED_", ""),
      PROFILE = case_when(
         PROFILE == "TB_PX" ~ "1",
         PROFILE == "PREGNANT" ~ "2",
         PROFILE == "HEP_B" ~ "3",
         PROFILE == "HEP_C" ~ "4",
         PROFILE == "CBS_REACTIVE" ~ "5",
         PROFILE == "PREP_PX" ~ "6",
         PROFILE == "PEP_PX" ~ "7",
         PROFILE == "STI" ~ "8",
         TRUE ~ PROFILE
      )
   ) %>%
   filter(IS_PROFILE == 1)

# test_reason
eharp$tables$px_test_reason <- eharp$data$records %>%
   select(
      REC_ID,
      CREATED_AT,
      CREATED_BY,
      starts_with("TEST_REASON")
   ) %>%
   pivot_longer(
      cols      = starts_with("TEST_REASON"),
      names_to  = "REASON",
      values_to = "IS_REASON"
   ) %>%
   mutate(
      REASON = stri_replace_all_regex(REASON, "^TEST_REASON_", ""),
      REASON = case_when(
         REASON == "HIV_EXPOSE" ~ "1",
         REASON == "PHYSICIAN" ~ "2",
         REASON == "HIV_PHYSICIAN" ~ "2",
         REASON == "EMPLOY_OFW" ~ "3",
         REASON == "EMPLOY_LOCAL" ~ "4",
         REASON == "INSURANCE" ~ "5",
         REASON == "NO_REASON" ~ "6",
         REASON == "RETEST" ~ "7",
         REASON == "OTHER" ~ "8888",
         REASON == "PEER_ED" ~ "8",
         REASON == "TEXT_EMAIL" ~ "9",
         TRUE ~ REASON
      )
   ) %>%
   filter(IS_REASON == 1) %>%
   left_join(
      y  = eharp$data$records %>%
         select(
            REC_ID,
            REASON_OTHER = TEST_OTHER_TEXT
         ) %>%
         mutate(REASON = "8888"),
      by = c("REC_ID", "REASON")
   )

# px_test & px_test_hiv
eharp$tables$px_test <- eharp$data$records %>%
   select(
      REC_ID,
      FACI_ID,
      CREATED_AT,
      CREATED_BY,
      PERFORM_BY   = SIGNATORY_1,
      DATE_PERFORM = RECORD_DATE,
      T31_RESULT   = T1_result,
      T32_RESULT   = T2_result,
      T33_RESULT   = T3_result,
      T10_RESULT   = screening_result
   ) %>%
   mutate_at(
      .vars = vars(ends_with("RESULT")),
      ~as.character(.)
   ) %>%
   pivot_longer(
      cols      = ends_with("RESULT"),
      names_to  = "TEST_TYPE",
      values_to = "RESULT"
   ) %>%
   mutate(
      TEST_NUM  = 1,
      TEST_TYPE = stri_replace_all_regex(TEST_TYPE, "[^[:digit:]]", ""),
      RESULT    = case_when(
         RESULT == "Reactive" ~ "1",
         RESULT == "Nonreactive" ~ "2",
         RESULT == "Inconclusive" ~ "3",
         RESULT == "0" ~ "2",
         TRUE ~ RESULT
      )
   ) %>%
   filter(!is.na(RESULT))

eharp$tables$px_test_hiv <- eharp$data$records %>%
   select(
      REC_ID,
      FACI_ID,
      CREATED_AT,
      CREATED_BY,
      SPECIMEN_TYPE    = sample_type,
      DATE_COLLECT,
      DATE_RECEIVE     = RECORD_DATE,
      T31_FINAL_RESULT = T1_result,
      T31_KIT_NAME     = T1_kit_name,
      T31_LOT_NO       = T1_lot_no,
      T32_KIT_NAME     = T2_kit_name,
      T32_FINAL_RESULT = T2_result,
      T32_LOT_NO       = T2_lot_no,
      T33_KIT_NAME     = T3_kit_name,
      T33_FINAL_RESULT = T3_result,
      T33_LOT_NO       = T3_lot_no,
      T33_p31          = p31,
      T33_gp160        = gp160,
      T33_p24          = p24,
      T33_gp41         = gp41,
      T33_HIV_1_RESULT = T3_HIV_1,
      T33_gp36         = gp36,
      T33_gp140        = gp140,
      T33_HIV_2_RESULT = T3_HIV_2,
   ) %>%
   mutate_at(
      .vars = vars(starts_with("T3")),
      ~as.character(.)
   ) %>%
   pivot_longer(
      cols      = starts_with("T3"),
      names_to  = "TEST_DATA",
      values_to = "TEST_VALUE"
   ) %>%
   mutate(
      TEST_TYPE = substr(TEST_DATA, 2, 3),
      PIECE     = substr(TEST_DATA, 5, 1000),
   ) %>%
   pivot_wider(
      id_cols     = c(REC_ID, FACI_ID, CREATED_AT, CREATED_BY, TEST_TYPE),
      names_from  = PIECE,
      values_from = TEST_VALUE
   ) %>%
   mutate_at(
      .vars = vars(HIV_1_RESULT, HIV_2_RESULT),
      ~case_when(
         . == "Reactive" ~ "1",
         . == "Nonreactive" ~ "2",
         . == "Inconclusive" ~ "3",
         . == "0" ~ "2",
         TRUE ~ .
      )
   ) %>%
   mutate(
      TEST_NUM     = 1,
      TEST_TYPE    = stri_replace_all_regex(TEST_TYPE, "[^[:digit:]]", ""),
      CONTROL      = 1,
      FINAL_RESULT = case_when(
         FINAL_RESULT == "Reactive" ~ "10",
         FINAL_RESULT == "Nonreactive" ~ "20",
         FINAL_RESULT == "Inconclusive" ~ "30",
         FINAL_RESULT == "0" ~ "20",
         FINAL_RESULT == "1" ~ "10",
         TRUE ~ FINAL_RESULT
      ),
      KIT_NAME     = case_when(
         KIT_NAME == "Alere Determine HIV 1/2" ~ "Alere Determine HIV-1/2",
         KIT_NAME == "Bio-Rad Geenius HIV 1/2 Confirmatory Assay" ~ "Geenius HIV 1/2 Confirmatory Assay",
         KIT_NAME == "SD Bioline HIV 1/2 3.0" ~ "SD Bioline HIV 1/2 3.0",
      )
   ) %>%
   filter(!is.na(FINAL_RESULT))

##  Upsert data ----------------------------------------------------------------

invisible(lapply(names(eharp$sql$wide), function(table) {
   id_cols <- eharp$sql$wide[[table]]
   data    <- eharp$tables[[table]]

   db_conn     <- ohasis$conn("db")
   table_space <- Id(schema = "ohasis_interim", table = table)
   dbxUpsert(
      db_conn,
      table_space,
      data,
      id_cols
   )
   dbDisconnect(db_conn)
}))

invisible(lapply(names(eharp$sql$long), function(table) {
   id_cols <- eharp$sql$long[[table]]
   data    <- eharp$tables[[table]]

   db_conn     <- ohasis$conn("db")
   table_space <- Id(schema = "ohasis_interim", table = table)
   dbxUpsert(
      db_conn,
      table_space,
      data,
      id_cols
   )
   dbDisconnect(db_conn)
}))
