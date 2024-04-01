##  CD4 Dataset ----------------------------------------------------------------

cd4          <- list()
cd4$yr       <- 2023
cd4$mo       <- 12
cd4$max_date <- end_ym(cd4$yr, cd4$mo)

lw_conn <- ohasis$conn("lw")
lab_cd4 <- QB$new(lw_conn)$
   from("ohasis_lake.lab_wide")$
   whereNotNull("LAB_CD4_DATE")$
   whereNotNull("LAB_CD4_RESULT", "or")$
   get()
id_reg  <- QB$new(lw_conn)$
   from("ohasis_warehouse.id_registry")$
   select(PATIENT_ID, CENTRAL_ID)$
   get()
dbDisconnect(lw_conn)

##  Registry Dataset -----------------------------------------------------------

cd4$harp_dx <- hs_data("harp_dx", "reg", cd4$yr, cd4$mo) %>%
   read_dta %>%
   zap_label %>%
   zap_formats %>%
   mutate_if(.predicate = is.character, ~na_if(., "")) %>%
   mutate(
      bdate = if_else(idnum == 8902, as.Date('1976-07-28'), bdate)
   ) %>%
   select(-CENTRAL_ID) %>%
   get_cid(id_reg, PATIENT_ID)

##  On ART Dataset -------------------------------------------------------------

cd4$harp_tx <- hs_data("harp_tx", "reg", cd4$yr, cd4$mo) %>%
   read_dta(col_select = c(art_id, confirmatory_code, birthdate, PATIENT_ID)) %>%
   left_join(read_dta(hs_data("harp_tx", "outcome", cd4$yr, cd4$mo)), join_by(art_id)) %>%
   zap_label %>%
   zap_formats %>%
   mutate_if(.predicate = is.character, ~na_if(., "")) %>%
   mutate(
      birthdate = if_else(idnum == 8902, as.Date('1976-07-28'), birthdate)
   ) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   arrange(desc(latest_nextpickup), desc(latest_ffupdate)) %>%
   group_by(CENTRAL_ID) %>%
   summarise(
      confirmatory_code = first(confirmatory_code, na.rm = TRUE),
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
      labcode = if_else(is.na(labcode), confirmatory_code, labcode)
   ) %>%
   select(-confirmatory_code)

##  CD4 Cleaned ----------------------------------------------------------------

cd4$long <- lab_cd4 %>%
   get_cid(id_reg, PATIENT_ID) %>%
   select(CENTRAL_ID, CD4_DATE = LAB_CD4_DATE, CD4_RESULT = LAB_CD4_RESULT) %>%
   mutate(
      # cd4 tagging
      CD4_RESULT   = stri_replace_all_charclass(CD4_RESULT, "[:alpha:]", ""),
      CD4_RESULT   = stri_replace_all_fixed(CD4_RESULT, " ", ""),
      CD4_RESULT   = stri_replace_all_fixed(CD4_RESULT, "<", ""),
      CD4_RESULT   = parse_number(CD4_RESULT),
      CD4_RESULT_C = case_when(
         CD4_RESULT >= 500 ~ 1,
         CD4_RESULT >= 350 & CD4_RESULT < 500 ~ 2,
         CD4_RESULT >= 200 & CD4_RESULT < 350 ~ 3,
         CD4_RESULT >= 50 & CD4_RESULT < 200 ~ 4,
         CD4_RESULT < 50 ~ 5,
      ),

      CD4_DATE     = as.Date(CD4_DATE)
   ) %>%
   filter(!is.na(CD4_RESULT), CD4_DATE <= cd4$max_date, CD4_RESULT >= 0) %>%
   distinct()

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
   left_join(
      y  = hs_data("harp_tx", "reg", cd4$yr, cd4$mo) %>%
         read_dta(col_select = c(art_id, PATIENT_ID)) %>%
         get_cid(id_reg, PATIENT_ID) %>%
         select(CENTRAL_ID, art_id),
      by = join_by(CENTRAL_ID)
   ) %>%
   relocate(art_id, .after = idnum) %>%
   remove_empty("cols")

write_dta(format_stata(cd4$data), "H:/cd4_harp_2023-12.dta")

stata(r"(
u "H:/cd4_harp_2023-12.dta", clear

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

ds, has(type string)
foreach var in `r(varlist)' {{
   loc type : type `var'
   loc len = substr("`type'", 4, 1000)

   cap form `var' %-`len's
}}

form *date* %tdCCYY-NN-DD
compress

sa "H:/20240321_cd4_harp_2022-12.dta", replace
)")