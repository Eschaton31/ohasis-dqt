dr <- new.env()

log_info("Downloading VL Data.")
local(envir = dr, {
   coverage <- list(
      min = "2022-01-01",
      max = "2023-04-30",
      yr  = 2023,
      mo  = 3
   )

   lw_conn <- ohasis$conn("lw")
   db_name <- "ohasis_warehouse"
   forms   <- list(
      id_reg = dbTable(lw_conn, db_name, "id_registry"),
      art_vl = dbTable(
         lw_conn,
         db_name,
         "form_art_bc",
         raw_where = TRUE,
         where     = glue(r"(
(LAB_VIRAL_DATE IS NOT NULL OR LAB_VIRAL_RESULT IS NOT NULL) AND
  (VISIT_DATE >= '{coverage$min}' OR LAB_VIRAL_DATE >= '{coverage$min}')
)"),
         cols      = c(
            "PATIENT_ID",
            "LAB_VIRAL_DATE",
            "LAB_VIRAL_RESULT",
            "FACI_ID",
            "SUB_FACI_ID",
            "SERVICE_FACI",
            "SERVICE_SUB_FACI"
         )
      )
   )
   dbDisconnect(lw_conn)
   rm(lw_conn, db_name)
})

log_info("Loading HARP Data.")
local(envir = dr, {
   ml_files <- list.files(Sys.getenv("HARP_VL"), "*vl_ml.*\\.dta", full.names = TRUE)
   data_ml  <- lapply(ml_files, function(ml) {
      qr <- substr(ml,
                   stri_locate_first_fixed(ml, "vl_ml_") + 6,
                   stri_locate_first_fixed(ml, ".dta") - 1)
      if (qr != "ever") {
         data <- read_dta(ml)
         if (!grepl("Q", qr)) {
            data %<>%
               select(-any_of("id")) %>%
               rename(
                  id = row_id
               )
         }
         data %<>%
            mutate(
               id = as.character(id),
               qr = qr
            )
      }
   })
   data_ml  <- bind_rows(data_ml)

   # get only needed columns
   data_ml %<>%
      select(
         PATIENT_ID,
         hub,
         src_file,
         src_sheet,
         qr,
         any_of("vlml2022"),
         starts_with("vl_result"),
         starts_with("vl_date")
      ) %>%
      mutate(
         hub    = toupper(hub),
         branch = NA_character_
      ) %>%
      faci_code_to_id(
         ohasis$ref_faci_code,
         c(FACI_ID = "hub", SUB_FACI_ID = "branch")
      ) %>%
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
      # filter(!is.na(PATIENT_ID), drop == 0) %>%
      filter(drop == 0) %>%
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
            vlml2022 == 1 ~ as.Date(vl_date),
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
      )

   harp <- list(
      tx = hs_data("harp_tx", "reg", coverage$yr, coverage$mo) %>%
         read_dta(
            col_select = c(
               PATIENT_ID,
               art_id,
               confirmatory_code,
               uic,
               sex
            )
         ) %>%
         left_join(
            y  = hs_data("harp_tx", "outcome", coverage$yr, coverage$mo) %>%
               read_dta(),
            by = join_by(art_id)
         ),
      ml = data_ml
   )
   rm(data_ml, ml_files)
})

##  clean data from forms

dr$data <- dr$forms$art_vl %>%
   get_cid(dr$forms$id_reg, PATIENT_ID) %>%
   select(
      CENTRAL_ID,
      FACI_ID,
      SUB_FACI_ID,
      SERVICE_FACI,
      SERVICE_SUB_FACI,
      vl_date_2 = LAB_VIRAL_DATE,
      vl_result = LAB_VIRAL_RESULT
   ) %>%
   mutate(
      res_tag = 2
   ) %>%
   bind_rows(
      dr$harp$ml %>%
         get_cid(dr$forms$id_reg, PATIENT_ID) %>%
         select(
            CENTRAL_ID,
            FACI_ID,
            SUB_FACI_ID,
            vl_date_2,
            vl_result,
            vl_result_alt,
            vlml2022
         ) %>%
         mutate(res_tag = 1)
   ) %>%
   distinct_all() %>%
   # clean results
   mutate(
      vl_result_encoded = vl_result,
      vl_result         = toupper(str_squish(vl_result)),
      vl_result         = stri_replace_all_fixed(vl_result, "\"", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV-1 VIRAL RNA DETECTED AT ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV - 1 DECTECTED ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV - 1 DETECTED ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV0-1 DETECTED ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV - 1 DETECTED", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV 1 DETECTED, ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV 1 DETECTED,", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV 1 DETECTED ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV -1 DETECTED ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV-1 DETECTED ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV-1 DETECTED, ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV DETECTED, ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV-1 DETECETED, ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV-1 DETECTED,", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV-DETECTED ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV 1DETECTED ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HOV-1 DETECTED ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV-1 DETECTED", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV1 DETECTED ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV1 DETECTED", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV DETECTED ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV DETECTED", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIVDETECTED ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIVDETECTED", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HI-1 DETECTED ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV-1 DETECETD, ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV 1- DETECTED ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HI-1 DETECTED", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV- DETECTED ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV-1 NO DETECTED", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "HIV 1 ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " COPIES/ML", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " COPPIES/ML", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "COPIES/ML", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " CP/ML", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " COPIES", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " CPM", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " COPIS/ML", ""),
      vl_result         = stri_replace_all_regex(vl_result, "^DETECTED ", ""),
      vl_result         = stri_replace_all_regex(vl_result, "^- ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "C/ML", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "CP/ML", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "COPIES", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "COIPES/ML", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " COPIE", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " CPIES", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " C0PIES", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " CPOIES", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " COIES", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "COIES", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " COPES", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " COPIS", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " XOPIES", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " COPPIES", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " CELLS", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " /ML", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " M/L", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " / ML", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "/ML", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "/ ML", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " PER ML", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " C/U", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "C/U", ""),
      vl_result         = stri_replace_all_fixed(vl_result, "ML", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " CPPIES", ""),
      vl_result         = stri_replace_all_fixed(vl_result, ", ", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " ,", ""),
      vl_result         = stri_replace_all_fixed(vl_result, " `", ""),
      vl_result         = stri_replace_last_regex(vl_result, "\\.00$", ""),
      vl_result         = stri_replace_last_regex(vl_result, "([:digit:])EO", "$1E0"),
      vl_result         = stri_replace_all_fixed(vl_result, "(12/11/2020) 2.51 E05 (LOG 5.40)", "2.51 E05 (LOG 5.40)"),

      # tag those w/ less than data
      less_than         = case_when(
         stri_detect_fixed(vl_result, ">") ~ 1,
         stri_detect_fixed(vl_result, "+") ~ 1,
         stri_detect_fixed(vl_result, "<") ~ 1,
         stri_detect_fixed(toupper(vl_result), "LESS THAN") ~ 1,
         stri_detect_fixed(toupper(vl_result), "LESS ") ~ 1,
         stri_detect_fixed(toupper(vl_result), "LES THAN") ~ 1,
         TRUE ~ 0
      ),
      has_alpha         = if_else(
         condition = stri_detect_regex(vl_result, "[:alpha:]"),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      sci_cal           = case_when(
         stri_detect_fixed(vl_result, "LOG") ~ "log",
         stri_detect_fixed(vl_result, "^") ~ "exponent",
         stri_detect_fixed(vl_result, "EO") ~ "eo",
         stri_detect_regex(vl_result, "[:digit:]E ") ~ "eo",
         stri_detect_regex(vl_result, "[:digit:]E") ~ "eo",
         stri_detect_fixed(vl_result, "X10") ~ "x10",
      ),

      vl_result_2       = case_when(
         vlml2022 == 1 ~ as.numeric(vl_result),
         sci_cal == "x10" ~ as.numeric(substr(vl_result, 1, stri_locate_first_fixed(vl_result, "X10") - 1)) * 10,
         sci_cal == "eo" ~ as.numeric(str_squish(substr(vl_result, 1, stri_locate_first_regex(vl_result, "[:digit:]E")))) * (10^as.numeric(substr(vl_result, stri_locate_first_regex(vl_result, "[:digit:]E") + 2, stri_locate_first_regex(vl_result, "[:digit:]E") + 4))),
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
         stri_detect_fixed(vl_result, "UNDETETABLE") ~ 0,
         stri_detect_fixed(vl_result, "UNDETECETD") ~ 0,
         stri_detect_fixed(vl_result, "NO MEASURABLE") ~ 0,
         stri_detect_fixed(vl_result, "NOT TEDECTED") ~ 0,
         stri_detect_fixed(vl_result, "NOTY DETECTED") ~ 0,
         str_detect(vl_result, glue("\\bND\\b")) ~ 0,
         vl_result %in% c("HND", "ND", "N/D", "UD", "TND", "UNDE", "UN", "N.D", "NP", "UNDECTABLE", "UNDECTED", "UNDET") ~ 0,
         vl_result %in% c("TARGET NOT DETECTED", "TARGET NOT DEFECTED", "TNDD", "TNDS", "U", "UNDETACTABLE", "UNDETE", "UNTEDECTABLE", "UP") ~ 0,
         vl_result %in% c("N", "NO DETECTED", "NO\\", "NONE DETECTED", "NOT", "NOT CONNECTED", "NOT DETECED", "NOT DTECTED", "HIV 1 NOT DTECTED") ~ 0,
         vl_result %in% c("BN", "BD") ~ 0,
         vl_result == "L40" ~ 39,
         vl_result == "L70" ~ 69,
         vl_result == "L70" ~ 69,
         vl_result %in% c("LT 34", "LT6 34") ~ 33,
         !is.na(as.numeric(stri_replace_all_fixed(vl_result, " ", ""))) ~ as.numeric(stri_replace_all_fixed(vl_result, " ", ""))
      ),

      # tag for dropping
      drop              = case_when(
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
      log_raw           = if_else(
         condition = stri_detect_fixed(vl_result, "LOG"),
         true      = substr(vl_result, 1, stri_locate_first_fixed(vl_result, "LOG") - 1),
         false     = NA_character_
      ),
      log_multiplier    = if_else(
         condition = stri_detect_fixed(log_raw, "E"),
         true      = substr(log_raw, stri_locate_first_fixed(log_raw, "E") + 1, nchar(log_raw)),
         false     = NA_character_
      ) %>%
         stri_replace_all_regex("[^[:digit:]]", ""),
      log_multiplier    = if_else(
         condition = !is.na(log_multiplier),
         true      = stri_pad_right("1", as.numeric(log_multiplier) + 1, "0"),
         false     = NA_character_
      ) %>% as.numeric(),
      log_raw           = if_else(
         condition = stri_detect_fixed(log_raw, "E"),
         true      = substr(log_raw, 1, stri_locate_first_fixed(log_raw, "E") - 1) %>% stri_replace_all_regex("[^[:digit:]]", ""),
         false     = log_raw
      ),

      log_increase      = if_else(
         condition = stri_detect_fixed(vl_result, "LOG"),
         true      = substr(vl_result, stri_locate_first_fixed(vl_result, "LOG"), nchar(vl_result)),
         false     = NA_character_
      ),
      log_increase      = stri_replace_all_fixed(log_increase, ")", ""),
      log_increase      = stri_replace_all_regex(log_increase, "[:alpha:]", ""),
      log_increase      = stri_replace_all_fixed(log_increase, " ", ""),
      log_increase      = as.numeric(log_increase) * 10,

      # apply calculations
      vl_result_2       = case_when(
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
   # filter(
   #    drop == 0,
   #    !is.na(vl_date_2),
   #    !is.na(vl_result_2),
   # ) %>%
   bind_rows(
      read_dta(file.path(Sys.getenv("HARP_VL"), "20220510_vl_ml_ever.dta")) %>%
         mutate(
            hub    = toupper(hub),
            hub    = case_when(
               hub == "AHD" ~ "ADH",
               hub == "P7S" ~ "PR7",
               hub == "PATH" ~ "PAT",
               TRUE ~ hub
            ),
            branch = NA_character_
         ) %>%
         faci_code_to_id(
            ohasis$ref_faci_code,
            c(FACI_ID = "hub", SUB_FACI_ID = "branch")
         ) %>%
         filter(PATIENT_ID != "") %>%
         mutate_if(
            .predicate = is.character,
            ~if_else(. == "", NA_character_, .)
         ) %>%
         get_cid(dr$forms$id_reg, PATIENT_ID) %>%
         select(
            CENTRAL_ID,
            FACI_ID,
            SUB_FACI_ID,
            vl_date_2,
            vl_result_2,
            res_tag
         )
   ) %>%
   mutate(
      drop              = coalesce(drop, 0),
      vl_result_encoded = coalesce(vl_result_encoded, as.character(vl_result_2))
   ) %>%
   mutate(
      FACI_ID_2     = FACI_ID,
      SUB_FACI_ID_2 = SUB_FACI_ID,
   ) %>%
   ohasis$get_faci(
      list("facility_name" = c("FACI_ID_2", "SUB_FACI_ID_2")),
      "name",
      c("vl_region", "vl_province", "vl_muncity")
   ) %>%
   mutate(
      FACI_ID_2     = FACI_ID,
      SUB_FACI_ID_2 = SUB_FACI_ID,
   ) %>%
   ohasis$get_faci(
      list("hub" = c("FACI_ID", "SUB_FACI_ID")),
      "code"
   ) %>%
   mutate(
      res_tag = labelled(
         res_tag,
         c(
            `ml`    = 1,
            `forms` = 2
         )
      )
   ) %>%
   select(
      CENTRAL_ID,
      FACI_ID               = FACI_ID_2,
      SUB_FACI_ID           = SUB_FACI_ID_2,
      hub,
      res_tag,
      vl_reporting_facility = facility_name,
      vl_region,
      vl_province,
      vl_muncity,
      vl_date               = vl_date_2,
      vl_result_encoded,
      vl_result_clean       = vl_result_2,
      vl_record_invalid     = drop
   ) %>%
   left_join(
      y = dr$harp$tx %>%
         get_cid(dr$forms$id_reg, PATIENT_ID) %>%
         faci_code_to_id(
            ohasis$ref_faci_code,
            c(TX_FACI = "hub", TX_SUB_FACI = "branch")
         ) %>%
         faci_code_to_id(
            ohasis$ref_faci_code,
            c(REAL_FACI = "realhub", REAL_SUB_FACI = "realhub_branch")
         ) %>%
         mutate_at(
            .vars = vars(TX_FACI, REAL_FACI),
            ~if_else(. == "130000", NA_character_, ., .),
         ) %>%
         ohasis$get_faci(
            list("txfaci" = c("TX_FACI", "TX_SUB_FACI")),
            "name",
            c("txfaci_region", "txfaci_province", "txfaci_muncity")
         ) %>%
         ohasis$get_faci(
            list("realfaci" = c("REAL_FACI", "REAL_SUB_FACI")),
            "name",
            c("realfaci_region", "realfaci_province", "realfaci_muncity")
         ) %>%
         distinct(art_id, .keep_all = TRUE) %>%
         select(
            CENTRAL_ID,
            art_id,
            artstart_date,
            hub_harp202303        = hub,
            txfaci_harp202303     = txfaci,
            txreg_harp202303      = txfaci_region,
            txprov_harp202303     = txfaci_province,
            txmunc_harp202303     = txfaci_muncity,
            realfaci_harp202303   = realfaci,
            realreg_harp202303    = realfaci_region,
            realprov_harp202303   = realfaci_province,
            realmunc_harp202303   = realfaci_muncity,
            ffup_harp202303       = latest_ffupdate,
            onart_harp202303      = onart,
            baselinevl_harp202303 = baseline_vl,
            vlp12m_harp202303     = vlp12m,
            vldate_harp202303     = vl_date,
            vlresult_harp202303   = vl_result,
         )
   )

dr$data %>%
   format_stata() %>%
   write_dta("H:/20230515_vldata_2023-04.dta")

dr$data %>%
   inner_join(
      read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw") %>%
         filter(site_epic_2022 == 1) %>%
         distinct(FACI_ID)
   ) %>%
   filter(
      vl_date %within% interval("2022-01-01", "2023-04-30")
   ) %>%
   format_stata() %>%
   write_dta("H:/20230515_vldata_2023-04_EpiC.dta")
