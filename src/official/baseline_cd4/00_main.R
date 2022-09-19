##  CD4 Dataset ----------------------------------------------------------------

cd4          <- list()
cd4$yr       <- 2021
cd4$max_date <- as.Date("2021-12-31")

##  Registry Dataset -----------------------------------------------------------

cd4$harp_dx <- hs_data("harp_dx", "reg", 2021, 12) %>%
   read_dta %>%
   zap_label %>%
   zap_formats %>%
   mutate_if(
      .predicate = is.character,
      ~if_else(. == '', NA_character_, .)
   ) %>%
   mutate(
      bdate = if_else(idnum == 8902, as.Date('1976-07-28'), bdate)
   # ) %>%
   # select(-CENTRAL_ID) %>%
   # left_join(
   #    y  = ohasis$db$registry %>% select(CENTRAL_ID, PATIENT_ID),
   #    by = "PATIENT_ID"
   )

##  On ART Dataset -------------------------------------------------------------

cd4$harp_tx <- hs_data("harp_tx", "outcome", 2021, 12)  %>%
   read_dta %>%
   zap_label %>%
   zap_formats %>%
   mutate_if(.predicate = is.character,
             ~if_else(. == '', NA_character_, .)) %>%
   mutate(
      birthdate = if_else(idnum == 8902, as.Date('1976-07-28'), birthdate)
   ) %>%
   rename(PATIENT_ID = oh_ci) %>%
   left_join(
      y  = ohasis$db$registry %>% select(CENTRAL_ID, PATIENT_ID),
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(is.na(CENTRAL_ID), ml_id, CENTRAL_ID)
   ) %>%
   arrange(desc(latest_nextpickup), desc(latest_ffupdate)) %>%
   group_by(CENTRAL_ID) %>%
   summarise(
      sacclcode         = first(sacclcode, na.rm = TRUE),
      artstart_date     = min(artstart_date, na.rm = TRUE),
      latest_ffupdate   = first(latest_ffupdate, na.rm = TRUE),
      latest_nextpickup = first(latest_nextpickup, na.rm = TRUE),
      latest_regimen    = first(latest_regimen, na.rm = TRUE),
      hub               = first(hub, na.rm = TRUE),
   )

##  Fully merged Dataset -------------------------------------------------------

cd4$harp_all <- cd4$harp_dx %>%
   select(
      idnum,
      year,
      datedx  = confirm_date,
      labcode = labcode2,
      CENTRAL_ID
   ) %>%
   full_join(
      y          = cd4$harp_tx,
      by         = "CENTRAL_ID",
      na_matches = "never"
   ) %>%
   mutate(
      labcode = if_else(is.na(labcode), sacclcode, labcode)
   ) %>%
   select(-sacclcode)

##  CD4 Cleaned ----------------------------------------------------------------

cd4$long <- readRDS("H:/Forms - CD4.RDS") %>%
   left_join(
      y  = ohasis$db$registry %>% select(CENTRAL_ID, PATIENT_ID),
      by = "PATIENT_ID"
   ) %>%
   select(-PATIENT_ID) %>%
   mutate(
      # cd4 tagging
      CD4_RESULT   = stri_replace_all_charclass(CD4_RESULT, "[:alpha:]", ""),
      CD4_RESULT   = stri_replace_all_fixed(CD4_RESULT, " ", ""),
      CD4_RESULT   = stri_replace_all_fixed(CD4_RESULT, "<", ""),
      CD4_RESULT   = as.numeric(CD4_RESULT),
      CD4_RESULT_C = case_when(
         CD4_RESULT >= 500 ~ 1,
         CD4_RESULT >= 350 & CD4_RESULT < 500 ~ 2,
         CD4_RESULT >= 200 & CD4_RESULT < 350 ~ 3,
         CD4_RESULT >= 50 & CD4_RESULT < 200 ~ 4,
         CD4_RESULT < 50 ~ 5,
      ),

      CD4_DATE     = as.Date(CD4_DATE)
   ) %>%
   filter(!is.na(CD4_RESULT), CD4_DATE <= cd4$max_date, CD4_RESULT >= 0)

##  CD4 History ----------------------------------------------------------------

cd4$wide <- cd4$long %>%
   group_by(CENTRAL_ID) %>%
   mutate(CD4_NUM = row_number()) %>%
   ungroup() %>%
   rename(
      cdd = CD4_DATE,
      cdr = CD4_RESULT,
   ) %>%
   pivot_wider(
      id_cols     = CENTRAL_ID,
      names_from  = CD4_NUM,
      values_from = c(cdd, cdr)
   ) %>%
   left_join(
      y  = cd4$long %>%
         group_by(CENTRAL_ID) %>%
         summarise(cd4_tests_done = n()),
      by = "CENTRAL_ID"
   )

##  Final Dataset --------------------------------------------------------------

cd4$data <- cd4$harp_all %>%
   left_join(
      y          = cd4$long %>%
         select(
            CENTRAL_ID,
            bcd4_d   = CD4_DATE,
            bcd4_r   = CD4_RESULT,
            bcd4_cat = CD4_RESULT_C
         ),
      by         = "CENTRAL_ID",
      na_matches = "never"
   ) %>%
   mutate(
      # calculate distance from confirmatory date
      # make values absolute to take date nearest to date
      CD4_CONFIRM = difftime(as.Date(datedx), bcd4_d, units = "days") %>%
         as.numeric() %>%
         abs(),

      # baseline is within 182 days
      bcd4        = if_else(
         CD4_CONFIRM <= 182,
         1,
         0
      ),
      bcd4_d      = if_else(bcd4 == 0, NA_Date_, bcd4_d),
      bcd4_r      = if_else(bcd4 == 0, as.numeric(NA), bcd4_r),
      bcd4_cat    = if_else(bcd4 == 0, as.numeric(NA), bcd4_cat),
   ) %>%
   arrange(idnum, CENTRAL_ID, CD4_CONFIRM) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   left_join(
      y          = cd4$long %>%
         select(
            CENTRAL_ID,
            ecd4_d   = CD4_DATE,
            ecd4_r   = CD4_RESULT,
            ecd4_cat = CD4_RESULT_C
         ),
      by         = "CENTRAL_ID",
      na_matches = "never"
   ) %>%
   mutate(
      # calculate distance from confirmatory date
      # make values absolute to take date nearest to date
      CD4_ARTSTART = difftime(as.Date(artstart_date), ecd4_d, units = "days") %>%
         as.numeric() %>%
         abs(),

      # baseline is within 182 days
      ecd4         = if_else(
         CD4_ARTSTART <= 182,
         1,
         0
      ),
      ecd4_d       = if_else(ecd4 == 0, NA_Date_, ecd4_d),
      ecd4_r       = if_else(ecd4 == 0, as.numeric(NA), ecd4_r),
      ecd4_cat     = if_else(ecd4 == 0, as.numeric(NA), ecd4_cat),
   ) %>%
   arrange(idnum, CENTRAL_ID, CD4_ARTSTART) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   select(-CD4_ARTSTART, CD4_CONFIRM) %>%
   left_join(
      y  = cd4$wide,
      by = "CENTRAL_ID"
   ) %>%
   remove_empty("cols")

write_dta(cd4$data, "H:/cd4_harp_2021-12.dta")

stata(r"(
u "H:/cd4_harp_2021-12.dta", clear

format_compress

label define baseline_cd4 1 `"1_500"', modify
label define baseline_cd4 2 `"2_350-499"', modify
label define baseline_cd4 3 `"3_200-349"', modify
label define baseline_cd4 4 `"4_50-199"', modify
label define baseline_cd4 5 `"5_below 50"', modify

lab val *_cat baseline_cd4

form *_d %tdCCYY-NN-DD
form cdd* %tdCCYY-NN-DD
form *date %tdCCYY-NN-DD
form *pickup %tdCCYY-NN-DD

sa "H:/20220311_cd4_harp_2021-12.dta", replace
)")