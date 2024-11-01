source("src/misc/ly_extracts/site_list.R")

con   <- connect("ohasis-lw")
forms <- QB$new(con)
# forms$whereBetween('VISIT_DATE', c(min, max))
forms$where(function(query = QB$new(con)) {
   query$whereIn('FACI_ID', sites$FACI_ID, boolean = "or")
   query$whereIn('SERVICE_FACI', sites$FACI_ID, boolean = "or")
   query$whereIn('CREATED_BY', staff$STAFF_ID, boolean = "or")
   query$whereIn('SERVICE_BY', staff$STAFF_ID, boolean = "or")
   query$whereNested
})

forms$from("ohasis_warehouse.form_prep")
form_prep <- forms$get()

rec_link <- QB$new(con)$from("ohasis_warehouse.rec_link")$get()
dbDisconnect(con)

prep_all <- read_dta(hs_data("prep", "reg", 2024, 7)) %>%
   get_cid(id_reg, PATIENT_ID)

ly_prep <- process_prep(form_prep, testing, rec_link) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   mutate(
      # name
      STANDARD_FIRST  = stri_trans_general(FIRST, "latin-ascii"),
      name            = str_squish(stri_c(LAST, ", ", FIRST, " ", MIDDLE, " ", SUFFIX)),

      # Age
      AGE             = coalesce(AGE, AGE_MO / 12),
      AGE_DTA         = calc_age(BIRTHDATE, VISIT_DATE),

      # tag those without PREP_FACI
      use_record_faci = if_else(is.na(SERVICE_FACI), 1, 0, 0),
      SERVICE_FACI    = if_else(use_record_faci == 1, FACI_ID, SERVICE_FACI),
   ) %>%
   ohasis$get_faci(
      list(PREP_HUB = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
      "name",
      c("PREP_REG", "PREP_PROV", "PREP_MUNC")
   )

ly_prep %>%
   mutate(
      prep_plan = case_when(
         prep_plan == "paid" ~ "PAID",
         prep_plan == "free" ~ "FREE",
      )
   ) %>%
   left_join(
      y  = prep_all %>%
         select(
            CENTRAL_ID,
            first,
            middle,
            last,
            suffix,
            uic,
            birthdate,
            sex
         ) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE),
      by = join_by(CENTRAL_ID)
   ) %>%
   relocate(
      first,
      middle,
      last,
      suffix,
      uic,
      birthdate,
      sex,
      .after = CENTRAL_ID
   ) %>%
   format_stata() %>%
   write_dta("H:/20240907_prep-ly_ever.dta")

ly_prep %>%
   mutate(
      prep_plan = case_when(
         prep_plan == "paid" ~ "PAID",
         prep_plan == "free" ~ "FREE",
      )
   ) %>%
   left_join(
      y  = prep_all %>%
         select(
            CENTRAL_ID,
            first,
            middle,
            last,
            suffix,
            uic,
            birthdate,
            sex
         ) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE),
      by = join_by(CENTRAL_ID)
   ) %>%
   relocate(
      first,
      middle,
      last,
      suffix,
      uic,
      birthdate,
      sex,
      .after = CENTRAL_ID
   ) %>%
   write_xlsx("H:/20240907_prep-ly_ever.xlsx")

ly_prep %>%
   tab(PREP_HUB)

ly_prep %>%
   group_by(PREP_HUB) %>%
   summarise(
      records  = n(),
      earliest = format(min(VISIT_DATE, na.rm = TRUE), "%b %d, %Y"),
      latest   = format(max(VISIT_DATE, na.rm = TRUE), "%b %d, %Y"),
   )

lw_conn <- ohasis$conn("lw")
dx      <- QB$new(lw_conn)$from("harp_dx.reg_202408")$get()
tx_reg  <- QB$new(lw_conn)$from("harp_tx.reg_202408")$get()
tx_out  <- QB$new(lw_conn)$from("harp_tx.outcome_202408")$get()
dbDisconnect(lw_conn)

ly_art_enroll <- tx_reg %>%
   filter(artstart_hub == "TLY") %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(FACI_ID = "artstart_hub", SUB_FACI_ID = "artstart_branch")
   ) %>%
   ohasis$get_faci(
      list(site_name = c("FACI_ID", "SUB_FACI_ID")),
      "name",
   ) %>%
   left_join(
      y  = dx %>%
         select(
            idnum,
            reg_sex = sex,
            transmit,
            self_identity,
            sexhow,
            region,
            province,
            muncity
         ),
      by = join_by(idnum)
   ) %>%
   mutate(
      # KAP
      msm            = case_when(
         reg_sex == "MALE" & sexhow %in% c("BISEXUAL", "HOMOSEXUAL") ~ 1,
         reg_sex == "MALE" & sexhow == "HETEROSEXUAL" ~ 0,
         TRUE ~ 0
      ),
      tg             = case_when(
         reg_sex == "MALE" & self_identity %in% c("FEMALE", "OTHERS") ~ 1,
         reg_sex == "FEMALE" & self_identity %in% c("MALE", "OTHERS") ~ 1,
         TRUE ~ 0
      ),
      hetero         = if_else(
         condition = sexhow == "HETEROSEXUAL",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      pwid           = if_else(
         condition = transmit == "IVDU",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      unknown        = case_when(
         transmit == "UNKNOWN" ~ 1,
         is.na(transmit) ~ 1,
         TRUE ~ 0
      ),
      Key_Population = case_when(
         msm == 1 ~ "MSM",
         reg_sex == "MALE" & tg == 1 ~ "TGW",
         reg_sex == "FEMALE" & tg == 1 ~ "TGM",
         pwid == 1 ~ "PWID",
         transmit == "SEX" & sex == "MALE" ~ "Hetero Male",
         transmit == "SEX" & sex == "FEMALE" ~ "Hetero Female",
         unknown == 1 ~ "Unknown",
         TRUE ~ "Others"
      ),


      AGE            = age,
      AGE            = case_when(
         art_id == 108586 ~ 20,
         TRUE ~ AGE
      ),
      AGE_BAND       = case_when(
         AGE %between% c(0, 14) ~ "1) <15",
         AGE %between% c(15, 17) ~ "2) 15-17",
         AGE %between% c(18, 24) ~ "3) 18-24",
         AGE %between% c(25, 34) ~ "4) 25-34",
         AGE %between% c(35, 49) ~ "5) 35-49",
         AGE %between% c(50, 1000) ~ "6) 50+",
         TRUE ~ "(no data)"
      ),
   ) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         PERM_PSGC_REG  = "region",
         PERM_PSGC_PROV = "province",
         PERM_PSGC_MUNC = "muncity"
      )
   ) %>%
   ohasis$get_addr(
      c(
         PERM_NAME_REG  = "PERM_PSGC_REG",
         PERM_NAME_PROV = "PERM_PSGC_PROV",
         PERM_NAME_MUNC = "PERM_PSGC_MUNC"
      ),
      "name"
   )

reg_names <- names(tx_reg)
out_names <- names(tx_out)
names     <- setdiff(out_names, reg_names)
names     <- c("art_id", names)

ly_art_curr <- tx_reg %>%
   inner_join(
      y  = tx_out %>%
         select(any_of(names)) %>%
         filter(realhub == "TLY"),
      by = join_by(art_id)
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(FACI_ID = "realhub", SUB_FACI_ID = "realhub_branch")
   ) %>%
   ohasis$get_faci(
      list(site_name = c("FACI_ID", "SUB_FACI_ID")),
      "name",
   ) %>%
   left_join(
      y  = dx %>%
         select(
            idnum,
            reg_sex = sex,
            transmit,
            self_identity,
            sexhow,
            region,
            province,
            muncity
         ),
      by = join_by(idnum)
   ) %>%
   mutate(
      # KAP
      msm            = case_when(
         reg_sex == "MALE" & sexhow %in% c("BISEXUAL", "HOMOSEXUAL") ~ 1,
         reg_sex == "MALE" & sexhow == "HETEROSEXUAL" ~ 0,
         TRUE ~ 0
      ),
      tg             = case_when(
         reg_sex == "MALE" & self_identity %in% c("FEMALE", "OTHERS") ~ 1,
         reg_sex == "FEMALE" & self_identity %in% c("MALE", "OTHERS") ~ 1,
         TRUE ~ 0
      ),
      hetero         = if_else(
         condition = sexhow == "HETEROSEXUAL",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      pwid           = if_else(
         condition = transmit == "IVDU",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      unknown        = case_when(
         transmit == "UNKNOWN" ~ 1,
         is.na(transmit) ~ 1,
         TRUE ~ 0
      ),
      Key_Population = case_when(
         msm == 1 ~ "MSM",
         reg_sex == "MALE" & tg == 1 ~ "TGW",
         reg_sex == "FEMALE" & tg == 1 ~ "TGM",
         pwid == 1 ~ "PWID",
         transmit == "SEX" & sex == "MALE" ~ "Hetero Male",
         transmit == "SEX" & sex == "FEMALE" ~ "Hetero Female",
         unknown == 1 ~ "Unknown",
         TRUE ~ "Others"
      ),


      AGE            = curr_age,
      AGE_BAND       = case_when(
         AGE %between% c(0, 14) ~ "1) <15",
         AGE %between% c(15, 17) ~ "2) 15-17",
         AGE %between% c(18, 24) ~ "3) 18-24",
         AGE %between% c(25, 34) ~ "4) 24-34",
         AGE %between% c(35, 49) ~ "5) 35-49",
         AGE %between% c(50, 1000) ~ "6) 50+",
         TRUE ~ "(no data)"
      ),
   ) %>%
   harp_addr_to_id(
      ohasis$ref_addr,
      c(
         PERM_PSGC_REG  = "region",
         PERM_PSGC_PROV = "province",
         PERM_PSGC_MUNC = "muncity"
      )
   ) %>%
   ohasis$get_addr(
      c(
         PERM_NAME_REG  = "PERM_PSGC_REG",
         PERM_NAME_PROV = "PERM_PSGC_PROV",
         PERM_NAME_MUNC = "PERM_PSGC_MUNC"
      ),
      "name"
   )

ly_art_enroll %>%
   group_by(AGE_BAND) %>%
   summarise(
      Enrolled = n()
   ) %>%
   adorn_totals() %>%
   write_clip()

ly_art_curr %>%
   group_by(PERM_NAME_REG, PERM_NAME_PROV, PERM_NAME_MUNC, outcome) %>%
   summarise(
      Clients = n()
   ) %>%
   pivot_wider(
      values_from = Clients,
      names_from  = outcome
   ) %>%
   adorn_totals() %>%
   write_clip()

ly_art_curr %>%
   mutate(
      vl_tested = if_else(outcome == "alive on arv" &
                             is.na(baseline_vl) &
                             !is.na(vlp12m), 1, 0, 0),
      vl_ud     = if_else(outcome == "alive on arv" &
                             is.na(baseline_vl) &
                             vlp12m == 1, 1, 0, 0),
      vl_sup    = if_else(outcome == "alive on arv" &
                             is.na(baseline_vl) &
                             !is.na(vlp12m) &
                             vl_result < 1000, 1, 0, 0),
   ) %>%
   group_by(PERM_NAME_REG, PERM_NAME_PROV, PERM_NAME_MUNC) %>%
   summarise(
      `alive on arv` = sum(if_else(outcome == "alive on arv", 1, 0)),
      vl_tested_p12m = sum(vl_tested),
      vl_sup         = sum(vl_sup),
      vl_ud          = sum(vl_ud),
   ) %>%
   adorn_totals() %>%
   mutate(
      `suppression rate (among tested)`  = vl_sup / vl_tested_p12m,
      `undetectable rate (among tested)` = vl_ud / vl_tested_p12m,
   ) %>%
   write_clip()


ly_art_curr %>%
   format_stata %>%
   write_dta("H:/20240921_tx-curr-ly_2024-08.dta")

ly_art_enroll %>%
   format_stata %>%
   write_dta("H:/20240921_tx-enroll-ly_2024-08.dta")

