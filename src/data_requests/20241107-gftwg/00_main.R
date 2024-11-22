sites <- read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw", "gf-city")

cleanFacilityIds <- function(data, faci_id, sub_faci_id) {
   data %<>%
      mutate(
         {{faci_id}}     := coalesce({{faci_id}}, ""),
         {{sub_faci_id}} := case_when(
            StrLeft({{sub_faci_id}}, 6) != {{faci_id}} ~ "",
            {{sub_faci_id}} == "130023_001" ~ "130023_001",
            StrLeft({{sub_faci_id}}, 6) %in% c("130001", "130605", "040200", "130797") ~ {{sub_faci_id}},
            TRUE ~ ""
         )
      )

   return(data)
}

yr       <- 2024
start_mo <- 7
end_mo   <- 10

min    <- as.character(start_ym(yr, start_mo))
max    <- as.character(end_ym(yr, end_mo))
period <- stri_c(tolower(month.abb[start_mo]), "-", tolower(month.abb[end_mo]), yr)
dir    <- file.path("H:/20241109-gftwg", period)
check_dir(dir)

# testing <- get_hts(min, max)
#
# con    <- ohasis$conn("lw")
# id_reg <- QB$new(con)$from("ohasis_warehouse.id_registry")$select("CENTRAL_ID", "PATIENT_ID")$get()
# dbDisconnect(con)

# write_rds(sites, file.path(dir, "sites.rds"))
# write_rds(testing, file.path(dir, "testing.rds"))
# write_rds(id_reg, file.path(dir, "id_reg.rds"))

sites   <- read_rds(file.path(dir, "sites.rds"))
testing <- read_rds(file.path(dir, "testing.rds"))
id_reg  <- read_rds(file.path(dir, "id_reg.rds"))

convert <- process_hts(testing$hts, testing$a, testing$cfbs) %>%
   get_cid(id_reg, PATIENT_ID)

check <- convert %>%
   mutate(
      FINAL_FACI = coalesce(SERVICE_FACI, SPECIMEN_SOURCE, FACI_ID),
      FINAL_SUB  = coalesce(SERVICE_SUB_FACI, SPECIMEN_SUB_SOURCE)
   ) %>%
   cleanFacilityIds(FINAL_FACI, FINAL_SUB) %>%
   left_join(
      y  = ohasis$ref_faci %>%
         select(
            FINAL_FACI = FACI_ID,
            FINAL_SUB  = SUB_FACI_ID,
            FACI_PSGC_REG,
            FACI_PSGC_PROV,
            FACI_PSGC_MUNC
         ),
      by = join_by(FINAL_FACI, FINAL_SUB)
   ) %>%
   mutate(
      use_cbs        = FACI_PSGC_REG == "990000000" & !is.na(HIV_SERVICE_PSGC_REG),
      FACI_PSGC_REG  = if_else(use_cbs, HIV_SERVICE_PSGC_REG, FACI_PSGC_REG, FACI_PSGC_REG),
      FACI_PSGC_PROV = if_else(use_cbs, HIV_SERVICE_PSGC_PROV, FACI_PSGC_PROV, FACI_PSGC_PROV),
      FACI_PSGC_MUNC = if_else(use_cbs, HIV_SERVICE_PSGC_MUNC, FACI_PSGC_MUNC, FACI_PSGC_MUNC),
   ) %>%
   mutate(rec = 1) %>%
   full_join(
      y  = sites %>%
         mutate(
            gf_site = 1
         ),
      by = join_by(
         FACI_PSGC_REG,
         FACI_PSGC_PROV,
         FACI_PSGC_MUNC
      )
   )

gf_test <- convert %>%
   mutate(
      FINAL_FACI = coalesce(SERVICE_FACI, SPECIMEN_SOURCE, FACI_ID),
      FINAL_SUB  = coalesce(SERVICE_SUB_FACI, SPECIMEN_SUB_SOURCE)
   ) %>%
   cleanFacilityIds(FINAL_FACI, FINAL_SUB) %>%
   left_join(
      y  = ohasis$ref_faci %>%
         select(
            FINAL_FACI = FACI_ID,
            FINAL_SUB  = SUB_FACI_ID,
            FACI_PSGC_REG,
            FACI_PSGC_PROV,
            FACI_PSGC_MUNC
         ),
      by = join_by(FINAL_FACI, FINAL_SUB)
   ) %>%
   mutate(
      use_cbs        = FACI_PSGC_REG == "990000000" & !is.na(HIV_SERVICE_PSGC_REG),
      FACI_PSGC_REG  = if_else(use_cbs, HIV_SERVICE_PSGC_REG, FACI_PSGC_REG, FACI_PSGC_REG),
      FACI_PSGC_PROV = if_else(use_cbs, HIV_SERVICE_PSGC_PROV, FACI_PSGC_PROV, FACI_PSGC_PROV),
      FACI_PSGC_MUNC = if_else(use_cbs, HIV_SERVICE_PSGC_MUNC, FACI_PSGC_MUNC, FACI_PSGC_MUNC),
   ) %>%
   left_join(
      y  = sites %>%
         select(
            FACI_PSGC_REG,
            FACI_PSGC_PROV,
            FACI_PSGC_MUNC
         ) %>%
         mutate(
            gf_site = 1
         ),
      by = join_by(
         FACI_PSGC_REG,
         FACI_PSGC_PROV,
         FACI_PSGC_MUNC
      )
   ) %>%
   select(
      -FACI_PSGC_REG,
      -FACI_PSGC_PROV,
      -FACI_PSGC_MUNC
   ) %>%
   get_latest_pii(
      "CENTRAL_ID",
      c(
         "BIRTHDATE",
         "SEX",
         "SELF_IDENT",
         "SELF_IDENT_OTHER",
         "CURR_PSGC_REG",
         "CURR_PSGC_PROV",
         "CURR_PSGC_MUNC",
         "PERM_PSGC_REG",
         "PERM_PSGC_PROV",
         "PERM_PSGC_MUNC",
         "BIRTH_PSGC_REG",
         "BIRTH_PSGC_PROV",
         "BIRTH_PSGC_MUNC"
      )
   )

gf_analysis <- gf_test %>%
   convert_hts("name") %>%
   mutate(
      male        = SEX == "1_Male",
      female      = SEX == "2_Female",

      man         = StrLeft(SELF_IDENT, 1) == "1",
      woman       = StrLeft(SELF_IDENT, 1) == "2",
      other_ident = StrLeft(SELF_IDENT, 1) == "3",

      sexwithm    = str_detect(stri_c(risk_sexwithm, risk_sexwithm_nocdm), "yes"),
      sexwithf    = str_detect(stri_c(risk_sexwithf, risk_sexwithf_nocdm), "yes"),
   ) %>%
   generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, gender_identity) %>%
   mutate_if(
      .predicate = is.logical,
      ~coalesce(., FALSE)
   ) %>%
   mutate(
      gender_identity = coalesce(gender_identity, "(no data)"),

      kp_pdl          = StrLeft(CLIENT_TYPE, 1) == "7" |
         str_detect(CBS_VENUE, "BJ[M|N]P") |
         (str_detect(CBS_VENUE, "CAMP") & !str_detect(CBS_VENUE, "NE.ADA")),
      kp_pdl          = coalesce(kp_pdl, FALSE),

      kp_pwid         = HTS_PROV == "Cebu" & str_detect(risk_injectdrug, "yes"),
      kp_pwid         = coalesce(kp_pwid, FALSE),

      kp_msm          = male &
         gender_identity != "Transgender woman" &
         sexwithm,
      kp_msm          = coalesce(kp_msm, FALSE),

      kp_tgw          = male &
         gender_identity == "Transgender woman" &
         sexwithm,
      kp_tgw          = coalesce(kp_tgw, FALSE),
   )

##  key population -------------------------------------------------------------

kpCids <- function(data, kp) {
   filtered <- data %>%
      filter({{kp}}) %>%
      distinct(CENTRAL_ID)

   return(filtered)
}

kps <- list(
   pdl  = kpCids(gf_analysis, kp_pdl),
   pwid = kpCids(gf_analysis, kp_pwid),
   msm  = kpCids(gf_analysis, kp_msm),
   tgw  = kpCids(gf_analysis, kp_tgw)
)

kp_data <- bind_rows(kps, .id = "kp") %>%
   mutate(
      kp_ = 1
   ) %>%
   pivot_wider(
      id_cols     = CENTRAL_ID,
      names_from  = kp,
      values_from = kp_
   ) %>%
   mutate(
      final_kp = case_when(
         pdl == 1 ~ "PDL",
         pwid == 1 ~ "PWID",
         tgw == 1 ~ "TGW",
         msm == 1 ~ "MSM"
      )
   )

non_kp <- gf_analysis %>%
   distinct(CENTRAL_ID) %>%
   anti_join(kp_data) %>%
   mutate(final_kp = "Non-KP")

kp_data %<>%
   bind_rows(non_kp)

# PDL > PWID > MSM > TGW

##  risks ----------------------------------------------------------------------

riskCids <- function(data, risk) {
   p12m <- data %>%
      filter(str_detect({{risk}}, "yes")) %>%
      filter({{risk}} != "yes-beyond_p12m") %>%
      distinct(CENTRAL_ID)
   ever <- data %>%
      filter(str_detect({{risk}}, "yes")) %>%
      distinct(CENTRAL_ID)

   return(list(p12m = p12m, ever = ever))
}

risks <- list(
   sexwithm       = riskCids(gf_analysis, risk_sexwithm),
   sexwithm_nocdm = riskCids(gf_analysis, risk_sexwithm_nocdm),
   injectdrug     = riskCids(gf_analysis, risk_injectdrug)
)

risks <- lapply(risks, bind_rows, .id = "when")
risks <- lapply(risks, mutate, risk = 1)
risks <- lapply(risks, pivot_wider, id_cols = CENTRAL_ID, names_from = when, values_from = risk)

risk_data <- bind_rows(risks, .id = "risk") %>%
   mutate(
      risk_ = 1
   ) %>%
   pivot_wider(
      id_cols     = CENTRAL_ID,
      names_from  = risk,
      values_from = c(ever, p12m)
   ) %>%
   mutate(
      final_sexwithm   = case_when(
         p12m_sexwithm == 1 ~ "p12m",
         p12m_sexwithm_nocdm == 1 ~ "p12m",
         ever_sexwithm == 1 ~ "ever",
         ever_sexwithm_nocdm == 1 ~ "ever",
         TRUE ~ NA_character_
      ),
      final_injectdrug = case_when(
         p12m_injectdrug == 1 ~ "p12m",
         ever_injectdrug == 1 ~ "ever",
         TRUE ~ NA_character_
      ),
   ) %>%
   select(
      CENTRAL_ID,
      starts_with("final")
   )

##  ages -----------------------------------------------------------------------

ageCids <- function(data, min, max) {
   filtered <- data %>%
      mutate(
         AGE_DTA = calc_age(BIRTHDATE, RECORD_DATE),
      ) %>%
      filter(AGE %between% c(min, max) | AGE_DTA %between% c(min, max)) %>%
      distinct(CENTRAL_ID)

   return(filtered)
}

ages <- list(
   adult = ageCids(gf_analysis, 15, 1000),
   ykp   = ageCids(gf_analysis, 15, 24)
)

age_data <- bind_rows(ages, .id = "age") %>%
   mutate(
      age_ = 1
   ) %>%
   pivot_wider(
      id_cols     = CENTRAL_ID,
      names_from  = age,
      values_from = age_
   )

##  reach / test ---------------------------------------------------------------

reaches <- list(
   reached       = gf_analysis %>%
      filter((SERVICE_HIV_101 == "1_Yes" &
         !is.na(SERVICE_CONDOMS) &
         !is.na(SERVICE_LUBES)) | (hts_result != "(no data)")) %>%
      distinct(CENTRAL_ID),
   tested        = gf_analysis %>%
      filter(hts_result != "(no data)") %>%
      distinct(CENTRAL_ID),
   reactive      = gf_analysis %>%
      filter(hts_result == "R") %>%
      distinct(CENTRAL_ID),
   nonreactive   = gf_analysis %>%
      filter(hts_result == "NR") %>%
      distinct(CENTRAL_ID),
   indeterminate = gf_analysis %>%
      filter(hts_result == "IND") %>%
      distinct(CENTRAL_ID)
)

reach_data <- bind_rows(reaches, .id = "reach") %>%
   mutate(
      reach_ = 1
   ) %>%
   pivot_wider(
      id_cols     = CENTRAL_ID,
      names_from  = reach,
      values_from = reach_
   )


##  final analysis -------------------------------------------------------------

total <- kp_data %>%
   left_join(
      y  = gf_analysis %>%
         filter(gf_site == 1) %>%
         distinct(CENTRAL_ID) %>%
         mutate(gf_site = 1),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = gf_analysis %>%
         filter(male) %>%
         distinct(CENTRAL_ID) %>%
         mutate(male = 1),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = gf_analysis %>%
         filter(female) %>%
         distinct(CENTRAL_ID) %>%
         mutate(female = 1),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = risk_data,
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = age_data,
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = reach_data,
      by = join_by(CENTRAL_ID)
   )

write_dta(format_stata(total), file.path("H:/20241109-gftwg", stri_c(period, ".dta")))
# write_dta(total, file.path(dir, "final-unique.dta"))
# write_xlsx(total, file.path(dir, "final-unique.xlsx"))
