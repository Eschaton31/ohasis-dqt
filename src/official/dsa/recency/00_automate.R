rt       <- new.env()
rt$ss    <- "1Zqku6Dsk6gykRd3wwcF0zeHv5iqh7_CA"
rt$wd    <- file.path("src", "official", "dsa", "recency")
rt$sites <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw") %>%
   filter(site_rt_2023 == 1) %>%
   distinct(FACI_ID, .keep_all = TRUE)

site_rt_forms <- function(faci_id, activation_date) {
   lw_conn   <- ohasis$conn("lw")
   db_name   <- "ohasis_warehouse"
   tbl_name  <- "hiv_recency"
   tbl_space <- Id(schema = db_name, table = tbl_name)

   # query for relevant testing records
   faci_sql <- r"(
   SELECT test.REC_ID,
          test.CONFIRM_FACI,
          test.CONFIRM_SUB_FACI,
          test.SPECIMEN_SOURCE,
          test.SPECIMEN_SUB_SOURCE,
          test.RT_AGREED,
          test.RT_DATE,
          test.RT_RESULT,
          test.RT_KIT,
          test.RT_VL_REQUESTED,
          test.RT_VL_DATE,
          test.RT_VL_RESULT
   FROM ohasis_lake.px_pii AS hts
            JOIN ohasis_lake.px_hiv_testing AS test ON hts.REC_ID = test.REC_ID
   WHERE COALESCE(test.SPECIMEN_SOURCE, test.CONFIRM_FACI) = ?
     AND COALESCE(test.DATE_COLLECT, hts.RECORD_DATE) >= ?
     AND test.CONFIRM_RESULT REGEXP 'Positive'
     AND (test.CONFIRM_FACI in ('070010', '070013', '060007', '060001', '060008', '060003') OR
          (test.CONFIRM_FACI = '130023' AND test.CONFIRM_SUB_FACI = '130023_001'))
   )"

   # creation of table for restarts
   if (dbExistsTable(lw_conn, tbl_space)) {
      dbExecute(lw_conn, glue("DELETE FROM {db_name}.{tbl_name} WHERE COALESCE(SPECIMEN_SOURCE, CONFIRM_FACI) = ?"), params = list(faci_id))
   } else {
      ref <- dbxSelect(lw_conn, str_c(faci_sql, " LIMIT 0"), params = list(faci_id, activation_date))
      ohasis$upsert(lw_conn, db_name, tbl_name, ref, "REC_ID")
   }

   dbExecute(lw_conn, glue(r"(INSERT INTO {db_name}.{tbl_name} {faci_sql})"), params = list(faci_id, activation_date))
   dbDisconnect(lw_conn)
}


lw_conn     <- ohasis$conn("lw")
db_name     <- "harp"
table_space <- Id(schema = db_name, table = "recency")
dbCreateTable(lw_conn, table_space, rt$data$final)
dbExecute(lw_conn, stri_c("ALTER TABLE harp.recency ADD PRIMARY KEY (REC_ID);"))
ohasis$upsert(lw_conn, "harp", "recency", rt$data$final, "REC_ID")
dbDisconnect(lw_conn)



trial <- apply(rt$sites, 1, function(row) {
   row <- as.list(row)

   log_info("Running {green(row$FACI_NAME)}.")
   data <- site_rt_forms(row$FACI_ID, row$rt_activation_date)
})

lw_conn <- ohasis$conn("lw")
id_reg  <- dbTable(lw_conn, "ohasis_warehouse", "id_registry", cols = c("PATIENT_ID", "CENTRAL_ID"))
lab_cd4 <- dbTable(lw_conn, "ohasis_lake", "lab_cd4", cols = c("PATIENT_ID", "CD4_DATE", "CD4_RESULT"))
dbDisconnect(lw_conn)

lab_cd4 %<>%
   get_cid(id_reg, PATIENT_ID)

dx <- hs_data("harp_dx", "reg", 2023, 6) %>%
   read_dta(col_select = c(idnum, labcode2, PATIENT_ID, confirm_date, year, month)) %>%
   mutate(
      HARP_INCLUSION_DATE = end_ym(year, month)
   ) %>%
   get_cid(id_reg, PATIENT_ID)

tx <- hs_data("harp_tx", "reg", 2023, 6) %>%
   read_dta(col_select = c(art_id, PATIENT_ID, artstart_date)) %>%
   get_cid(id_reg, PATIENT_ID)


hts_data <- bind_rows(trial) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   left_join(dx %>% select(CENTRAL_ID, HARP_INCLUSION_DATE, labcode2, confirm_date), join_by(CENTRAL_ID)) %>%
   left_join(tx %>% select(CENTRAL_ID, artstart_date), join_by(CENTRAL_ID)) %>%
   mutate(
      days_from_tx = interval(DATE_CONFIRM, artstart_date) / days(1),
      dx_before    = if_else(CONFIRM_CODE != labcode2, 1, 0),
      tx_before    = if_else(days_from_tx < 0, 1, 0),
      everonart    = if_else(!is.na(artstart_date), 1, 0)
   )


hts_risk <- hts_data %>%
   select(
      REC_ID,
      contains("risk", ignore.case = FALSE)
   ) %>%
   pivot_longer(
      cols = contains("risk", ignore.case = FALSE)
   ) %>%
   group_by(REC_ID) %>%
   summarise(
      risks = stri_c(collapse = ", ", unique(sort(value)))
   )

hts_data %<>%
   get_cid(id_reg, PATIENT_ID) %>%
   left_join(
      y  = hts_risk,
      by = join_by(REC_ID)
   ) %>%
   remove_pii() %>%
   mutate(
      FORM_ENCODED = if_else(!is.na(FORM_VERSION), '1_Yes', "0_No", "0_No"),
      .after       = RT_RESULT
   ) %>%
   mutate(
      CBS_VENUE      = toupper(str_squish(HIV_SERVICE_ADDR)),
      ONLINE_APP     = case_when(
         grepl("GRINDR", CBS_VENUE) ~ "GRINDR",
         grepl("GRNDR", CBS_VENUE) ~ "GRINDR",
         grepl("GRINDER", CBS_VENUE) ~ "GRINDR",
         grepl("TWITTER", CBS_VENUE) ~ "TWITTER",
         grepl("FACEBOOK", CBS_VENUE) ~ "FACEBOOK",
         grepl("MESSENGER", CBS_VENUE) ~ "FACEBOOK",
         grepl("\\bFB\\b", CBS_VENUE) ~ "FACEBOOK",
         grepl("\\bGR\\b", CBS_VENUE) ~ "GRINDR",
      ),
      REACH_ONLINE   = if_else(!is.na(ONLINE_APP), "1_Yes", REACH_ONLINE, REACH_ONLINE),
      REACH_CLINICAL = if_else(
         condition = if_all(starts_with("REACH_"), ~is.na(.)) & hts_modality == "FBT",
         true      = "1_Yes",
         false     = REACH_CLINICAL,
         missing   = REACH_CLINICAL
      ),
      hts_date       = coalesce(hts_date, RECORD_DATE),
   ) %>%
   mutate(
      SEXUAL_RISK = case_when(
         str_detect(risk_sexwithm, "yes") & str_detect(risk_sexwithf, "yes") ~ "M+F",
         str_detect(risk_sexwithm, "yes") & !str_detect(risk_sexwithf, "yes") ~ "M",
         !str_detect(risk_sexwithm, "yes") & str_detect(risk_sexwithf, "yes") ~ "F",
      ),
      KAP_TYPE    = case_when(
         risks == "(no data)" | is.na(risks) ~ "(no data)",
         risks == "none" ~ "No apparent risk",
         risks == "(no data), no, none" ~ "No apparent risk",
         SEX == "MALE" &
            SEXUAL_RISK %in% c("M", "M+F") &
            str_detect(risk_injectdrug, "yes") ~ "MSM-PWID",
         SEX == "MALE" &
            SEXUAL_RISK == "F" &
            str_detect(risk_injectdrug, "yes") ~ "Hetero Male-PWID",
         SEX == "FEMALE" &
            !is.na(SEXUAL_RISK) &
            str_detect(risk_injectdrug, "yes") ~ "Hetero Female-PWID",
         SEX == "MALE" &
            SEXUAL_RISK %in% c("M", "M+F") &
            !str_detect(risk_injectdrug, "yes") ~ "MSM",
         SEX == "MALE" &
            SEXUAL_RISK == "F" &
            !str_detect(risk_injectdrug, "yes") ~ "Hetero Male",
         SEX == "FEMALE" &
            !is.na(SEXUAL_RISK) &
            !str_detect(risk_injectdrug, "yes") ~ "Hetero Female",
         str_detect(risk_injectdrug, "yes") ~ "PWID",
         str_detect(risk_needlestick, "yes") ~ "Occupational (Needlestick)",
         str_detect(risk_bloodtransfuse, "yes") ~ "Blood transfusion",
         TRUE ~ "(unclassified)"
      )
   ) %>%
   rename(
      CREATED = CREATED_BY,
      UPDATED = UPDATED_BY,
   ) %>%
   generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, gender_identity) %>%
   rename(
      HTS_PROVIDER_TYPE       = PROVIDER_TYPE,
      HTS_PROVIDER_TYPE_OTHER = PROVIDER_TYPE_OTHER,
   ) %>%
   process_vl("RT_VL_RESULT", "RT_VL_RESULT_CLEAN") %>%
   relocate(RT_VL_RESULT_CLEAN, .after = RT_VL_RESULT) %>%
   mutate(
      RITA_RESULT = case_when(
         RT_VL_RESULT_CLEAN >= 1000 ~ "1_Recent",
         RT_VL_RESULT_CLEAN < 1000 ~ "2_Long-term",
      ),
      RT_FACI     = coalesce(SPECIMEN_SOURCE, SERVICE_FACI, CONFIRM_FACI),
      RT_SUB_FACI = coalesce(SPECIMEN_SUB_SOURCE, SERVICE_SUB_FACI, CONFIRM_SUB_FACI),
      .after      = RT_VL_RESULT_CLEAN,
   ) %>%
   relocate(HARP_INCLUSION_DATE, .after = DATE_CONFIRM) %>%
   select(
      -any_of(
         c(
            "PRIME",
            "RECORD_DATE",
            "DISEASE",
            "BIRTHDATE",
            "HIV_SERVICE_TYPE",
            "GENDER_AFFIRM_THERAPY",
            "HIV_SERVICE_ADDR",
            "src",
            "MODULE",
            "MODALITY",
            "FACI_ID",
            "SUB_FACI_ID",
            "FORM_VERSION",
            "CONFIRMATORY_CODE",
            "DELETED_BY",
            "DELETED_AT",
            "SCREEN_AGREED",
            "EXPOSE_SEX_M_NOCONDOM",
            "EXPOSE_SEX_F_NOCONDOM",
            "EXPOSE_SEX_HIV",
            "AGE_FIRST_SEX",
            "NUM_F_PARTNER",
            "YR_LAST_F",
            "NUM_M_PARTNER",
            "YR_LAST_M",
            "AGE_FIRST_INJECT",
            "MED_CBS_REACTIVE",
            "MED_IS_PREGNANT",
            "FORMA_MSM",
            "FORMA_TGW",
            "FORMA_PWID",
            "FORMA_FSW",
            "FORMA_GENPOP",
            "SCREEN_REFER",
            "PARTNER_REFERRAL_FACI",
            "EXPOSE_SEX_EVER",
            "EXPOSE_CONDOMLESS_ANAL",
            "EXPOSE_CONDOMLESS_VAGINAL",
            "EXPOSE_M_SEX_ORAL_ANAL",
            "EXPOSE_NEEDLE_SHARE",
            "EXPOSE_ILLICIT_DRUGS",
            "EXPOSE_SEX_HIV_DATE",
            "EXPOSE_CONDOMLESS_ANAL_DATE",
            "EXPOSE_CONDOMLESS_VAGINAL_DATE",
            "EXPOSE_NEEDLE_SHARE_DATE",
            "EXPOSE_ILLICIT_DRUGS_DATE",
            "SERVICE_GIVEN_CONDOMS",
            "SERVICE_GIVEN_LUBES",
            "TEST_REFUSE_NO_TIME",
            "TEST_REFUSE_OTHER",
            "TEST_REFUSE_NO_CURE",
            "TEST_REFUSE_FEAR_RESULT",
            "TEST_REFUSE_FEAR_DISCLOSE",
            "TEST_REFUSE_FEAR_MSM",
            "CFBS_MSM",
            "CFBS_TGW",
            "CFBS_PWID",
            "CFBS_FSW",
            "CFBS_GENPOP"
         )
      )
   ) %>%
   mutate(
      row_id = row_number()
   )

total <- nrow(hts_data)
pb    <- progress_bar$new(format = ":current of :total rows | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = total, width = 100, clear = FALSE)
pb$tick(0)
for (i in seq_len(total)) {
   hts_data[i, 'CD4_DATE']   <- get_baseline_cd4(hts_data[i,]$CENTRAL_ID, hts_data[i,]$DATE_CONFIRM, lab_cd4, "date")
   hts_data[i, 'CD4_RESULT'] <- get_baseline_cd4(hts_data[i,]$CENTRAL_ID, hts_data[i,]$DATE_CONFIRM, lab_cd4, "result")
   pb$tick(1)
}

hts_data %<>%
   mutate(
      baseline_cd4 = case_when(
         CD4_RESULT >= 500 ~ 1,
         CD4_RESULT >= 350 & CD4_RESULT < 500 ~ 2,
         CD4_RESULT >= 200 & CD4_RESULT < 350 ~ 3,
         CD4_RESULT >= 50 & CD4_RESULT < 200 ~ 4,
         CD4_RESULT < 50 ~ 5,
      ),

      baseline_cd4 = labelled(
         baseline_cd4,
         c(
            "1_500+"     = 1,
            "2_350-499"  = 2,
            "3_200-349"  = 3,
            "4_50-199"   = 4,
            "5_below 50" = 5
         )
      ),
   )


get_baseline_cd4 <- function(cid, ref_date, lab_cd4, request = "date") {
   if (!is.Date(ref_date))
      ref_date <- as.Date(ref_date)

   min <- ref_date %m-% days(182)
   max <- ref_date %m+% days(182)

   ref_data <- lab_cd4 %>%
      filter(
         CENTRAL_ID == cid,
         CD4_DATE %within% interval(min, max)
      ) %>%
      mutate(
         CD4_DISTANCE = interval(CD4_DATE, ref_date) / days(1),
         CD4_DISTANCE = abs(CD4_DISTANCE)
      ) %>%
      arrange(CD4_DISTANCE, desc(CD4_DATE)) %>%
      slice(1)

   if (nrow(ref_data) > 0) {
      requested <- switch(
         request,
         date   = ref_data$CD4_DATE,
         result = ref_data$CD4_RESULT,
      )
   } else {
      requested <- switch(
         request,
         date   = NA_Date_,
         result = NA_character_,
      )
   }

   return(requested)
}


##  For Gab & Lala ------

lw_conn     <- ohasis$conn("lw")
db_name     <- "harp"
table_space <- Id(schema = db_name, table = "recency")
if (dbExistsTable(lw_conn, table_space))
   dbRemoveTable(lw_conn, table_space)

upload <- recency$official$recency %>%
   select(
      -starts_with("EXPOSE_"),
      -starts_with("TEST_REASON_"),
   ) %>%
   remove_pii()

# dbCreateTable(lw_conn, table_space, upload)
# dbExecute(lw_conn, stri_c("ALTER TABLE harp.recency ADD PRIMARY KEY (REC_ID);"))
ohasis$upsert(lw_conn, "harp", "recency", upload, "REC_ID")
dbDisconnect(lw_conn)

##  For AEM Subnational --------------------------------------------------------

ref <- psgc_aem(ohasis$ref_addr)
dx  <- list(
   `2021` = read_dta(hs_data("harp_full", "reg", 2021, 12)) %>% mutate(end_ref = as.Date("2021-12-31")),
   `2022` = read_dta(hs_data("harp_full", "reg", 2022, 12)) %>% mutate(end_ref = as.Date("2022-12-31")),
   `2023` = read_dta(hs_data("harp_full", "reg", 2023, 6)) %>% mutate(end_ref = as.Date("2023-06-30"))
)
dx  <- lapply(dx, function(data, ref) {
   data %<>%
      select(-starts_with("PSGC")) %>%
      harp_addr_to_id(
         ohasis$ref_addr,
         c(
            PERM_REG  = "region",
            PERM_PROV = "province",
            PERM_MUNC = "muncity"
         ),
         aem_sub_ntl = TRUE
      ) %>%
      select(-region, -province, -muncity) %>%
      left_join(
         y  = ref$addr %>%
            select(
               PERM_REG  = PSGC_REG,
               PERM_PROV = PSGC_PROV,
               PERM_MUNC = PSGC_MUNC,
               region    = NHSSS_REG,
               province  = NHSSS_PROV,
               muncity   = NHSSS_AEM
            ) %>%
            mutate_at(
               .vars = vars(starts_with("PERM_")),
               ~str_replace_all(., "^PH", "")
            ),
         by = join_by(PERM_REG, PERM_PROV, PERM_MUNC)
      ) %>%
      mutate(
         mortality   = if_else(dead == 1 | outcome == "dead", 1, 0, 0),
         dx          = if_else(!is.na(idnum), 1, 0, 0),
         dx_plhiv    = if_else(!is.na(idnum) & dead == 0, 1, 0, 0),
         plhiv       = if_else(mortality == 0, 1, 0, 0),

         onart       = case_when(
            outcome == "alive on arv" ~ 1,
            TRUE ~ 0
         ),
         dx15        = if_else(plhiv == 1 & cur_age >= 15, 1, 0, 0),
         dx          = if_else(plhiv == 1, 1, 0, 0),
         everonart   = if_else(plhiv == 1 & everonart == 1, 1, 0, 0),
         onart       = if_else(plhiv == 1 & onart == 1, 1, 0, 0),
         vl_tested   = if_else(plhiv == 1 &
                                  onart == 1 &
                                  is.na(baseline_vl) &
                                  !is.na(vlp12m), 1, 0, 0),
         vl_suppress = if_else(plhiv == 1 &
                                  onart == 1 &
                                  is.na(baseline_vl) &
                                  vlp12m == 1, 1, 0, 0),
      )
}, ref = ref)

dx$`2021` %>%
   group_by(region, province, muncity) %>%
   summarise_at(
      .vars = vars(dx15, dx, everonart, onart, vl_tested, vl_suppress),
      list(
         `2021` = ~sum(.)
      )
   ) %>%
   ungroup() %>%
   full_join(
      y  = dx$`2022` %>%
         group_by(region, province, muncity) %>%
         summarise_at(
            .vars = vars(dx15, dx, everonart, onart, vl_tested, vl_suppress),
            list(
               `2022` = ~sum(.)
            )
         ) %>%
         ungroup(),
      by = join_by(region, province, muncity)
   ) %>%
   ungroup() %>%
   full_join(
      y  = dx$`2023` %>%
         group_by(region, province, muncity) %>%
         summarise_at(
            .vars = vars(dx15, dx, everonart, onart, vl_tested, vl_suppress),
            list(
               `2023` = ~sum(.)
            )
         ) %>%
         ungroup(),
      by = join_by(region, province, muncity)
   ) %>%
   mutate(
      id = str_c(region, province, muncity)
   ) %>%
   select(
      region,
      province,
      muncity,
      id,
      starts_with("dx15_"),
      starts_with("dx_"),
      starts_with("everonart"),
      starts_with("onart"),
      starts_with("vl_tested"),
      starts_with("vl_suppress"),
   ) %>%
   write_sheet("1I-KM6JP8E1kaT_VwAJnMcqliQDjjdfhtIV2ytBoQKC4", "runs")
