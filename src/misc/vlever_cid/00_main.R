##  Process forms data ---------------------------------------------------------

start_vl    <- "2004-01-01"
lw_conn     <- ohasis$conn("lw")
form_art_bc <- dbTable(
   lw_conn,
   "ohasis_warehouse",
   "form_art_bc",
   cols      = c("PATIENT_ID", "VISIT_DATE", "LAB_VIRAL_DATE", "LAB_VIRAL_RESULT"),
   raw_where = TRUE,
   where     = glue(r"(
(LAB_VIRAL_DATE IS NOT NULL OR LAB_VIRAL_RESULT IS NOT NULL) AND
(VISIT_DATE >= '{start_vl}' OR LAB_VIRAL_DATE >= '{start_vl}')
   )")
)
id_registry <- dbTable(
   lw_conn,
   "ohasis_warehouse",
   "id_registry",
   cols = c("PATIENT_ID", "CENTRAL_ID")
)
dbDisconnect(lw_conn)

# Form BC + Lab Data
.log_info("Processing forms data.")
data_forms <- form_art_bc %>%
   # get latest central ids
   left_join(
      y  = id_registry,
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   ) %>%
   select(
      CENTRAL_ID,
      vl_date_2 = LAB_VIRAL_DATE,
      vl_result = LAB_VIRAL_RESULT
   )

##  Get masterlist data from the past 4 quarters -------------------------------

vl_yr   <- 2022
prev_yr <- 2021
# calculate datasets needed
data_ml <- read_dta("C:/data/harp_vl/data/20220810_vl_ml_2022-Q3.dta") %>%
   mutate(qr = "2022-Q3") %>%
   bind_rows(
      read_dta("C:/data/harp_vl/data/20220810_vl_ml_2022-Q2.dta") %>%
         mutate(qr = "2022-Q2")
   ) %>%
   bind_rows(
      read_dta("C:/data/harp_vl/data/20220510_vl_ml_2022-Q1.dta") %>%
         mutate(qr = "2022-Q1")
   ) %>%
   bind_rows(
      read_dta("C:/data/harp_vl/data/20220510_vl_ml_2021-Q4.dta") %>%
         mutate(qr = "2021-Q4")
   ) %>%
   bind_rows(
      read_dta("C:/data/harp_vl/data/20220510_vl_ml_2021-Q3.dta") %>%
         mutate(qr = "2021-Q3")
   ) %>%
   bind_rows(
      read_dta("C:/data/harp_vl/data/20220510_vl_ml_2021-Q2.dta") %>%
         mutate(qr = "2021-Q2")
   ) %>%
   bind_rows(
      read_dta("C:/data/harp_vl/data/20220510_vl_ml_ever.dta") %>%
         mutate(
            id = as.character(id)
         )
   )

# get only needed columns
data_ml %<>%
   select(
      PATIENT_ID,
      hub,
      src_file,
      src_sheet,
      qr,
      starts_with("vl_result"),
      starts_with("vl_date")
   )

##  Process masterlist data ----------------------------------------------------

.log_info("Processing massterlist data.")
data_ml %<>%
   mutate_if(
      .predicate = is.character,
      ~if_else(. == "", NA_character_, as.character(.))
   ) %>%
   # tag data to be dropped
   mutate(
      drop = case_when(
         toupper(vl_date) %in% c("DECEASED", "LTFU", "TRANS OUT", "FOR VL AFTER 2 MONTHS", "HIV 1 NOT DET", "NON-REACTIVE") ~ 1,
         TRUE ~ 0
      )
   ) %>%
   # drop data w/o matched PATIENT_ID
   filter(!is.na(PATIENT_ID), drop == 0) %>%
   mutate(
      # generate row_id for the appended data
      px_hub        = glue("{PATIENT_ID}{hub}{qr}"),

      # fix dates first
      first_slash   = stri_locate_first_fixed(vl_date, "/")[, 1],
      first_dash    = stri_locate_first_fixed(vl_date, "-")[, 1],
      first_dot     = stri_locate_first_fixed(vl_date, ".")[, 1],
      vl_excel_date = if_else(
         condition = StrIsNumeric(vl_date),
         true      = excel_numeric_to_date(as.numeric(vl_date)),
         false     = NA_Date_
      ),
      month_name    = case_when(
         stri_detect_fixed(toupper(vl_date), toupper(month.name[1])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[2])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[3])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[4])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[5])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[6])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[7])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[8])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[9])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[10])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[11])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.name[12])) ~ 1,
         TRUE ~ 0
      ),
      month_abb     = case_when(
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[1])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[2])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[3])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[4])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[5])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[6])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[7])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[8])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[9])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[10])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[11])) ~ 1,
         stri_detect_fixed(toupper(vl_date), toupper(month.abb[12])) ~ 1,
         TRUE ~ 0
      ),
      vl_date_2     = case_when(
         !is.na(vl_excel_date) ~ vl_excel_date,
         stri_detect_fixed(vl_date, "/") & hub == "tch" ~ as.Date(StrLeft(vl_date, 10), "%d/%m/%Y"),
         stri_detect_fixed(vl_date, "/") & hub == "dcs" ~ as.Date(StrLeft(vl_date, 10), "%d/%m/%Y"),
         stri_detect_fixed(vl_date, "(") & hub == "ccc" ~ as.Date(substr(vl_result, 2, 11), "%m/%d/%Y"),
         stri_detect_fixed(vl_date, "-") & StrIsNumeric(StrLeft(vl_date, 4)) ~ as.Date(StrLeft(vl_date, 10), "%Y-%m-%d"),
         stri_detect_fixed(vl_date, "/") & StrIsNumeric(StrLeft(vl_date, 4)) ~ as.Date(StrLeft(vl_date, 10), "%Y/%m/%d"),
         first_slash %in% c(2, 3) ~ as.Date(StrLeft(vl_date, 10), "%m/%d/%Y"),
         first_dash %in% c(2, 3) ~ as.Date(StrLeft(vl_date, 10), "%m-%d-%Y"),
         first_dot %in% c(2, 3) ~ as.Date(StrLeft(vl_date, 10), "%m.%d.%Y"),
         stri_detect_fixed(vl_date, ",") & month_name == 1 ~ as.Date(vl_date, "%B %d, %Y"),
         !stri_detect_fixed(vl_date, ",") & month_name == 1 ~ as.Date(vl_date, "%B %d %Y"),
         stri_detect_fixed(vl_date, ",") & month_abb == 1 ~ as.Date(vl_date, "%B %d, %Y"),
         !stri_detect_fixed(vl_date, ",") & month_abb == 1 ~ as.Date(vl_date, "%B %d %Y"),
      ),
   ) %>%
   left_join(
      y  = id_registry,
      by = "PATIENT_ID"
   )

data_ml <- .cleaning_list(data_ml, nhsss$harp_vl$corr$vl_ml, "PX_HUB", "character")

##  Append data and clean results ----------------------------------------------

vl_data <- data_forms %>%
   # res_tag <- check source of the data
   mutate(res_tag = 2) %>%
   bind_rows(
      data_ml %>%
         select(
            CENTRAL_ID,
            vl_date_2,
            vl_result,
            vl_result_alt
         ) %>%
         mutate(res_tag = 1)
   ) %>%
   distinct_all() %>%
   # clean results
   mutate(
      vl_result      = toupper(str_squish(vl_result)),
      vl_result      = stri_replace_all_fixed(vl_result, "\"", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV-1 VIRAL RNA DETECTED AT ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV - 1 DECTECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV - 1 DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV0-1 DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV - 1 DETECTED", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV 1 DETECTED, ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV 1 DETECTED,", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV 1 DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV -1 DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV-1 DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV-1 DETECTED, ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV DETECTED, ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV-1 DETECETED, ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV-1 DETECTED,", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV-1 DETECTED", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV1 DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV1 DETECTED", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV DETECTED", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIVDETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIVDETECTED", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HI-1 DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV-1 DETECETD, ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV 1- DETECTED ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "HIV 1 ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " COPIES/ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " COPPIES/ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "COPIES/ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " CP/ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " COPIES", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " CPM", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " COPIS/ML", ""),
      vl_result      = stri_replace_all_regex(vl_result, "^DETECTED ", ""),
      vl_result      = stri_replace_all_regex(vl_result, "^- ", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "C/ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "CP/ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "COPIES", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "COIPES/ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " COPIE", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " CPIES", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " /ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " M/L", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " / ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "/ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "/ ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " PER ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " C/U", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "C/U", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "ML", ""),
      vl_result      = stri_replace_all_fixed(vl_result, " CPPIES", ""),
      vl_result      = stri_replace_all_fixed(vl_result, "(12/11/2020) 2.51 E05 (LOG 5.40)", "2.51 E05 (LOG 5.40)"),

      # tag those w/ less than data
      less_than      = case_when(
         stri_detect_fixed(vl_result, ">") ~ 1,
         stri_detect_fixed(vl_result, "<") ~ 1,
         stri_detect_fixed(toupper(vl_result), "LESS THAN") ~ 1,
         stri_detect_fixed(toupper(vl_result), "LESS ") ~ 1,
         stri_detect_fixed(toupper(vl_result), "LES THAN") ~ 1,
         TRUE ~ 0
      ),
      has_alpha      = if_else(
         condition = stri_detect_regex(vl_result, "[:alpha:]"),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      sci_cal        = case_when(
         stri_detect_fixed(vl_result, "LOG") ~ "log",
         stri_detect_fixed(vl_result, "^") ~ "exponent",
         stri_detect_fixed(vl_result, "EO") ~ "eo",
         stri_detect_regex(vl_result, "[:digit:]E ") ~ "eo",
         stri_detect_regex(vl_result, "[:digit:]E") ~ "eo",
         stri_detect_fixed(vl_result, "X10") ~ "x10",
      ),

      vl_result_2    = case_when(
         less_than == 1 &
            stri_detect_fixed(vl_result, "40") &
            stri_detect_fixed(vl_result, "1.6") ~ 39,
         less_than == 1 & has_alpha == 0 ~ as.numeric(stri_replace_all_regex(vl_result, "[^[:digit:]]", "")) - 1,
         less_than == 1 & has_alpha == 1 & is.na(sci_cal) ~ as.numeric(stri_replace_all_regex(vl_result, "[^[:digit:]]", "")) - 1,
         less_than == 0 &
            has_alpha == 1 &
            is.na(sci_cal) &
            stri_detect_regex(vl_result, "M$") ~ as.numeric(stri_replace_all_regex(vl_result, "[^[:digit:]]", "")) * 1000000,
         StrIsNumeric(vl_result) ~ as.numeric(vl_result),
         stri_detect_fixed(vl_result, ", ") & has_alpha == 0 ~ as.numeric(stri_replace_all_fixed(vl_result, ", ", "")),
         stri_detect_fixed(vl_result, ",") & has_alpha == 0 ~ as.numeric(stri_replace_all_fixed(vl_result, ",", "")),
         stri_detect_fixed(vl_result, "NO T DET") ~ 0,
         stri_detect_fixed(vl_result, "NOT DET") ~ 0,
         stri_detect_fixed(vl_result, "MOT DET") ~ 0,
         stri_detect_fixed(vl_result, "NOT DETETED") ~ 0,
         stri_detect_fixed(vl_result, "UNDETECT") ~ 0,
         stri_detect_fixed(vl_result, "NO MEASURABLE") ~ 0,
         str_detect(vl_result, glue("\\bND\\b")) ~ 0,
         vl_result %in% c("HND", "ND", "N/D", "UD", "TND", "UNDE", "UN", "N.D", "NP", "UNDECTABLE", "UNDECTED", "UNDET") ~ 0,
         vl_result %in% c("TARGET NOT DETECTED", "TARGET NOT DEFECTED", "TNDD", "TNDS", "U", "UNDETACTABLE", "UNDETE", "UNTEDECTABLE", "UP") ~ 0,
         vl_result %in% c("N", "NO DETECTED", "NO\\", "NONE DETECTED", "NOT", "NOT CONNECTED", "NOT DETECED", "NOT DTECTED", "HIV 1 NOT DTECTED") ~ 0,
         vl_result %in% c("BN", "BD") ~ 0,
         vl_result == "L40" ~ 39,
         vl_result == "L70" ~ 69,
         vl_result == "L70" ~ 69,
         vl_result %in% c("LT 34", "LT6 34") ~ 33,
      ),

      # tag for dropping
      drop           = case_when(
         vl_result == "NOT DONE" ~ 1,
         vl_result == "PENDING" ~ 1,
         vl_result == "TO SECURE" ~ 1,
         vl_result == "NO RESULT" ~ 1,
         vl_result == "NON-REACTIVE" ~ 1,
         vl_result == "N/A" ~ 1,
         vl_result == "NONE" ~ 1,
         vl_result == "REPEAT COLLECTION" ~ 1,
         vl_result == "RC" ~ 1,
         vl_result == "NR" ~ 1,
         vl_result == "DONE.K.SILUNGAN" ~ 1,
         vl_result == "MTB DETECTED" ~ 1,
         vl_result == "INVALID" ~ 1,
         vl_result == "CPPIES" ~ 1,
         vl_result == "HIV I DETECTED" ~ 1,
         vl_result == "DETECTED" ~ 1,
         vl_result == "NA" ~ 1,
         vl_result == "O" ~ 1,
         vl_result == "T" ~ 1,
         vl_result == "." ~ 1,
         vl_result == "*" ~ 1,
         vl_result == "<" ~ 1,
         stri_detect_fixed(vl_result, "WAITING FOR") ~ 1,
         stri_detect_fixed(vl_result, "NOT SUFFICIENT") ~ 1,
         stri_detect_fixed(vl_result, "NOT YET") ~ 1,
         stri_detect_fixed(vl_result, "NO RESULT") ~ 1,
         stri_detect_fixed(vl_result, "TO FOLLOW") ~ 1,
         stri_detect_fixed(vl_result, "TO SECURE") ~ 1,
         stri_detect_fixed(vl_result, "ERROR") ~ 1,
         stri_detect_fixed(vl_result, "AWAITING") ~ 1,
         stri_detect_fixed(vl_result, "CP# 09750471506") ~ 1,
         is.na(vl_result) & is.na(vl_result_alt) ~ 1,
         TRUE ~ 0
      ),

      # log increase
      log_raw        = if_else(
         condition = stri_detect_fixed(vl_result, "LOG"),
         true      = substr(vl_result, 1, stri_locate_first_fixed(vl_result, "LOG") - 1),
         false     = NA_character_
      ),
      log_multiplier = if_else(
         condition = stri_detect_fixed(log_raw, "E"),
         true      = substr(log_raw, stri_locate_first_fixed(log_raw, "E") + 1, nchar(log_raw)),
         false     = NA_character_
      ) %>%
         stri_replace_all_regex("[^[:digit:]]", ""),
      log_multiplier = if_else(
         condition = !is.na(log_multiplier),
         true      = stri_pad_right("1", as.numeric(log_multiplier) + 1, "0"),
         false     = NA_character_
      ) %>% as.numeric(),
      log_raw        = if_else(
         condition = stri_detect_fixed(log_raw, "E"),
         true      = substr(log_raw, 1, stri_locate_first_fixed(log_raw, "E") - 1) %>% stri_replace_all_regex("[^[:digit:]]", ""),
         false     = log_raw
      ),

      log_increase   = if_else(
         condition = stri_detect_fixed(vl_result, "LOG"),
         true      = substr(vl_result, stri_locate_first_fixed(vl_result, "LOG"), nchar(vl_result)),
         false     = NA_character_
      ),
      log_increase   = stri_replace_all_fixed(log_increase, ")", ""),
      log_increase   = stri_replace_all_regex(log_increase, "[:alpha:]", ""),
      log_increase   = stri_replace_all_fixed(log_increase, " ", ""),
      log_increase   = as.numeric(log_increase) * 10,

      # apply calculations
      vl_result_2    = case_when(
         vl_result_2 == 99999 ~ 0,
         !is.na(log_increase) &
            is.na(log_multiplier) &
            !stri_detect_fixed(vl_result, "X") &
            !stri_detect_fixed(vl_result, "E") &
            !stri_detect_fixed(vl_result, "^") ~ as.numeric(stri_replace_all_regex(log_raw, "[^[:digit:]]", "")) * log_increase,
         !is.na(log_increase) &
            !is.na(log_multiplier) &
            stri_detect_fixed(vl_result, "E") &
            !stri_detect_fixed(vl_result, "X") &
            !stri_detect_fixed(vl_result, "^") ~ as.numeric(stri_replace_all_regex(log_raw, "[^[:digit:]]", "")) *
            log_increase *
            log_multiplier,
         is.na(vl_result_2) &
            stri_count_fixed(vl_result, ".") > 1 &
            StrIsNumeric(stri_replace_all_fixed(vl_result, ".", "")) ~ as.numeric(stri_replace_all_regex(vl_result, ".", "")),
         TRUE ~ vl_result_2
      )
   ) %>%
   # remove for dropping data
   filter(
      drop == 0,
      !is.na(vl_date_2),
      !is.na(vl_result_2),
   ) %>%
   bind_rows(
      read_dta("C:/data/harp_vl/data/20220510_vl_ml_ever.dta") %>%
         filter(PATIENT_ID != "") %>%
         mutate_if(
            .predicate = is.character,
            ~if_else(. == "", NA_character_, .)
         ) %>%
         left_join(
            y  = id_registry,
            by = "PATIENT_ID"
         ) %>%
         select(
            CENTRAL_ID,
            vl_date_2,
            vl_result_2,
            res_tag
         )
   )

end_vl <- "2022-09-30"
vl_filtered <- vl_data %>%
   mutate(
      drop = case_when(
         year(vl_date_2) < 2004 ~ 1,
         vl_date_2 > as.Date(end_vl) ~ 1,
         TRUE ~ 0
      )
   ) %>%
   filter(drop == 0)

vl_first <- vl_filtered %>%
   arrange(vl_date_2) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   select(
      CENTRAL_ID,
      vl_date_first   = vl_date_2,
      vl_result_first = vl_result_2
   )
vl_last  <- vl_filtered %>%
   arrange(vl_date_2) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   select(
      CENTRAL_ID,
      vl_date_last   = vl_date_2,
      vl_result_last = vl_result_2
   )

vl_final <- vl_first %>%
   full_join(vl_last)

dx    <- read_dta("C:/data/harp_full/data/20221021_harp_2022-09_wVL.dta")
tx    <- read_dta("C:/data/harp_tx/data/20221020_reg-art_2022-09.dta")
vl_dx <- dx %>%
   select(-contains("CENTRAL_ID")) %>%
   # get latest central ids
   left_join(
      y  = id_registry,
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   ) %>%
   left_join(vl_final) %>%
   mutate(
      newdata         = case_when(
         difftime(vl_date_last, artstart_date, units = "days") <= 92 & is.na(vl_date) ~ 1,
         difftime(vl_date_last, artstart_date, units = "days") <= 92 & vl_date_last > vl_date ~ 1,
         vl_date_last > vl_date ~ 1,
         TRUE ~ 0
      ),
      vl_date_first   = case_when(
         newdata == 1 ~ vl_date,
         is.na(vl_date_first) & !is.na(vl_result) ~ vl_date,
         TRUE ~ vl_date_first
      ),
      vl_date_last    = case_when(
         newdata == 1 ~ vl_date,
         is.na(vl_date_last) & !is.na(vl_result) ~ vl_date,
         TRUE ~ vl_date_last
      ),
      vl_result_first = case_when(
         newdata == 1 ~ vl_result,
         is.na(vl_result_first) & !is.na(vl_result) ~ vl_result,
         TRUE ~ vl_result_first
      ),
      vl_result_last  = case_when(
         newdata == 1 ~ vl_result,
         is.na(vl_result_last) & !is.na(vl_result) ~ vl_result,
         TRUE ~ vl_result_last
      ),

      vl_naive        = case_when(
         is.na(vl_date_first) &
            is.na(vl_date_last) &
            is.na(vl_date) ~ 1,
         TRUE ~ 0
      )
   ) %>%
   select(
      idnum,
      vl_naive,
      vl_date_first,
      vl_result_first,
      vl_date_last,
      vl_result_last,
   ) %>%
   distinct(idnum, .keep_all = TRUE)

vl_tx <- tx %>%
   select(-contains("CENTRAL_ID")) %>%
   left_join(
      y  = read_dta("C:/data/harp_tx/data/20221021_onart-vl_2022-09.dta", col_select = c(art_id, vl_date, vl_result)),
      by = "art_id"
   ) %>%
   # get latest central ids
   left_join(
      y  = id_registry,
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   ) %>%
   left_join(vl_final) %>%
   mutate(
      newdata         = case_when(
         difftime(vl_date_last, artstart_date, units = "days") <= 92 & is.na(vl_date) ~ 1,
         difftime(vl_date_last, artstart_date, units = "days") <= 92 & vl_date_last > vl_date ~ 1,
         vl_date_last > vl_date ~ 1,
         TRUE ~ 0
      ),
      vl_date_first   = case_when(
         newdata == 1 ~ vl_date,
         is.na(vl_date_first) & !is.na(vl_result) ~ vl_date,
         TRUE ~ vl_date_first
      ),
      vl_date_last    = case_when(
         newdata == 1 ~ vl_date,
         is.na(vl_date_last) & !is.na(vl_result) ~ vl_date,
         TRUE ~ vl_date_last
      ),
      vl_result_first = case_when(
         newdata == 1 ~ vl_result,
         is.na(vl_result_first) & !is.na(vl_result) ~ vl_result,
         TRUE ~ vl_result_first
      ),
      vl_result_last  = case_when(
         newdata == 1 ~ vl_result,
         is.na(vl_result_last) & !is.na(vl_result) ~ vl_result,
         TRUE ~ vl_result_last
      ),

      vl_naive        = case_when(
         is.na(vl_date_first) &
            is.na(vl_date_last) &
            is.na(vl_date) ~ 1,
         TRUE ~ 0
      )
   ) %>%
   select(
      art_id,
      vl_naive,
      vl_date_first,
      vl_result_first,
      vl_date_last,
      vl_result_last,
   ) %>%
   distinct(art_id, .keep_all = TRUE)