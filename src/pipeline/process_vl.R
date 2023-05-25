process_vl <- function(data, result_old, result_new) {
   vl_column_old <- as.name(result_old)
   vl_column_new <- as.name(result_new)
   data %<>%
      # clean results
      mutate(
         VL_RES         = toupper(str_squish(!!vl_column_old)),
         VL_RES         = stri_replace_all_fixed(VL_RES, "\"", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV-1 VIRAL RNA DETECTED AT ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV - 1 DECTECTED ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV - 1 DETECTED ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV0-1 DETECTED ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV - 1 DETECTED", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV 1 DETECTED, ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV 1 DETECTED,", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV 1 DETECTED ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV -1 DETECTED ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV-1 DETECTED ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV-1 DETECTED, ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV DETECTED, ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV-1 DETECETED, ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV-1 DETECTED,", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV-DETECTED ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV 1DETECTED ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HOV-1 DETECTED ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV-1 DETECTED", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV1 DETECTED ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV1 DETECTED", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV DETECTED ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV DETECTED", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIVDETECTED ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIVDETECTED", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HI-1 DETECTED ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV-1 DETECETD, ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV 1- DETECTED ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HI-1 DETECTED", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV- DETECTED ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV-1 NO DETECTED", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "HIV 1 ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " COPIES/ML", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " COPPIES/ML", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "COPIES/ML", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " CP/ML", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " COPIES", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " CPM", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " COPIS/ML", ""),
         VL_RES         = stri_replace_all_regex(VL_RES, "^DETECTED ", ""),
         VL_RES         = stri_replace_all_regex(VL_RES, "^- ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "C/ML", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "CP/ML", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "COPIES", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "COIPES/ML", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " COPIE", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " CPIES", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " C0PIES", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " CPOIES", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " COIES", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "COIES", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " COPES", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " COPIS", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " XOPIES", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " COPPIES", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " CELLS", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " /ML", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " M/L", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " / ML", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "/ML", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "/ ML", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " PER ML", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " C/U", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "C/U", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, "ML", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " CPPIES", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, ", ", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " ,", ""),
         VL_RES         = stri_replace_all_fixed(VL_RES, " `", ""),
         VL_RES         = stri_replace_last_regex(VL_RES, "\\.00$", ""),
         VL_RES         = stri_replace_last_regex(VL_RES, "([:digit:])EO", "$1E0"),
         VL_RES         = stri_replace_all_fixed(VL_RES, "(12/11/2020) 2.51 E05 (LOG 5.40)", "2.51 E05 (LOG 5.40)"),

         # tag those w/ less than data
         less_than      = case_when(
            stri_detect_fixed(VL_RES, ">") ~ 1,
            stri_detect_fixed(VL_RES, "+") ~ 1,
            stri_detect_fixed(VL_RES, "<") ~ 1,
            stri_detect_fixed(toupper(VL_RES), "LESS THAN") ~ 1,
            stri_detect_fixed(toupper(VL_RES), "LESS ") ~ 1,
            stri_detect_fixed(toupper(VL_RES), "LES THAN") ~ 1,
            TRUE ~ 0
         ),
         has_alpha      = if_else(
            condition = stri_detect_regex(VL_RES, "[:alpha:]"),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         sci_cal        = case_when(
            stri_detect_fixed(VL_RES, "LOG") ~ "log",
            stri_detect_fixed(VL_RES, "^") ~ "exponent",
            stri_detect_fixed(VL_RES, "EO") ~ "eo",
            stri_detect_regex(VL_RES, "[:digit:]E ") ~ "eo",
            stri_detect_regex(VL_RES, "[:digit:]E") ~ "eo",
            stri_detect_fixed(VL_RES, "X10") ~ "x10",
         ),

         VL_RES_2       = case_when(
            # vlml2022 == 1 ~ as.numeric(VL_RES),
            sci_cal == "x10" ~ as.numeric(substr(VL_RES, 1, stri_locate_first_fixed(VL_RES, "X10") - 1)) * 10,
            sci_cal == "eo" ~ as.numeric(str_squish(substr(VL_RES, 1, stri_locate_first_regex(VL_RES, "[:digit:]E")))) * (10^as.numeric(substr(VL_RES, stri_locate_first_regex(VL_RES, "[:digit:]E") + 2, stri_locate_first_regex(VL_RES, "[:digit:]E") + 4))),
            less_than == 1 &
               stri_detect_fixed(VL_RES, "40") &
               stri_detect_fixed(VL_RES, "1.6") ~ 39,
            less_than == 1 & has_alpha == 0 ~ as.numeric(stri_replace_all_regex(VL_RES, "[^[:digit:]]", "")) - 1,
            less_than == 1 & has_alpha == 1 & is.na(sci_cal) ~ as.numeric(stri_replace_all_regex(VL_RES, "[^[:digit:]]", "")) - 1,
            less_than == 0 &
               has_alpha == 1 &
               is.na(sci_cal) &
               stri_detect_regex(VL_RES, "M$") ~ as.numeric(stri_replace_all_regex(VL_RES, "[^[:digit:]]", "")) * 1000000,
            StrIsNumeric(VL_RES) ~ as.numeric(VL_RES),
            stri_detect_fixed(VL_RES, ", ") & has_alpha == 0 ~ as.numeric(stri_replace_all_fixed(VL_RES, ", ", "")),
            stri_detect_fixed(VL_RES, ",") & has_alpha == 0 ~ as.numeric(stri_replace_all_fixed(VL_RES, ",", "")),
            stri_detect_fixed(VL_RES, "NO T DET") ~ 0,
            stri_detect_fixed(VL_RES, "NOT DET") ~ 0,
            stri_detect_fixed(VL_RES, "MOT DET") ~ 0,
            stri_detect_fixed(VL_RES, "NOT DETETED") ~ 0,
            stri_detect_fixed(VL_RES, "UNDETECT") ~ 0,
            stri_detect_fixed(VL_RES, "UNDETETABLE") ~ 0,
            stri_detect_fixed(VL_RES, "UNDETECETD") ~ 0,
            stri_detect_fixed(VL_RES, "NO MEASURABLE") ~ 0,
            stri_detect_fixed(VL_RES, "NOT TEDECTED") ~ 0,
            stri_detect_fixed(VL_RES, "NOTY DETECTED") ~ 0,
            str_detect(VL_RES, glue("\\bND\\b")) ~ 0,
            VL_RES %in% c("HND", "ND", "N/D", "UD", "TND", "UNDE", "UN", "N.D", "NP", "UNDECTABLE", "UNDECTED", "UNDET") ~ 0,
            VL_RES %in% c("TARGET NOT DETECTED", "TARGET NOT DEFECTED", "TNDD", "TNDS", "U", "UNDETACTABLE", "UNDETE", "UNTEDECTABLE", "UP") ~ 0,
            VL_RES %in% c("N", "NO DETECTED", "NO\\", "NONE DETECTED", "NOT", "NOT CONNECTED", "NOT DETECED", "NOT DTECTED", "HIV 1 NOT DTECTED") ~ 0,
            VL_RES %in% c("BN", "BD") ~ 0,
            VL_RES == "L40" ~ 39,
            VL_RES == "L70" ~ 69,
            VL_RES == "L70" ~ 69,
            VL_RES %in% c("LT 34", "LT6 34") ~ 33,
            !is.na(as.numeric(stri_replace_all_fixed(VL_RES, " ", ""))) ~ as.numeric(stri_replace_all_fixed(VL_RES, " ", ""))
         ),

         # tag for dropping
         VL_DROP        = case_when(
            VL_RES == "NOT DONE" ~ 1,
            VL_RES == "PENDING" ~ 1,
            VL_RES == "TO SECURE" ~ 1,
            VL_RES == "NO RESULT" ~ 1,
            VL_RES == "NON-REACTIVE" ~ 1,
            VL_RES == "N/A" ~ 1,
            VL_RES == "NONE" ~ 1,
            VL_RES == "REPEAT COLLECTION" ~ 1,
            VL_RES == "RC" ~ 1,
            VL_RES == "NR" ~ 1,
            VL_RES == "DONE.K.SILUNGAN" ~ 1,
            VL_RES == "MTB DETECTED" ~ 1,
            VL_RES == "INVALID" ~ 1,
            VL_RES == "CPPIES" ~ 1,
            VL_RES == "HIV I DETECTED" ~ 1,
            VL_RES == "DETECTED" ~ 1,
            VL_RES == "NA" ~ 1,
            VL_RES == "O" ~ 1,
            VL_RES == "T" ~ 1,
            VL_RES == "." ~ 1,
            VL_RES == "*" ~ 1,
            VL_RES == "<" ~ 1,
            stri_detect_fixed(VL_RES, "WAITING FOR") ~ 1,
            stri_detect_fixed(VL_RES, "NOT SUFFICIENT") ~ 1,
            stri_detect_fixed(VL_RES, "NOT YET") ~ 1,
            stri_detect_fixed(VL_RES, "NO RESULT") ~ 1,
            stri_detect_fixed(VL_RES, "TO FOLLOW") ~ 1,
            stri_detect_fixed(VL_RES, "TO SECURE") ~ 1,
            stri_detect_fixed(VL_RES, "ERROR") ~ 1,
            stri_detect_fixed(VL_RES, "AWAITING") ~ 1,
            stri_detect_fixed(VL_RES, "CP# 09750471506") ~ 1,
            # is.na(VL_RES) & is.na(VL_RES_alt) ~ 1,
            TRUE ~ 0
         ),

         # log increase
         log_raw        = if_else(
            condition = stri_detect_fixed(VL_RES, "LOG"),
            true      = substr(VL_RES, 1, stri_locate_first_fixed(VL_RES, "LOG") - 1),
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
            condition = stri_detect_fixed(VL_RES, "LOG"),
            true      = substr(VL_RES, stri_locate_first_fixed(VL_RES, "LOG"), nchar(VL_RES)),
            false     = NA_character_
         ),
         log_increase   = stri_replace_all_fixed(log_increase, ")", ""),
         log_increase   = stri_replace_all_regex(log_increase, "[:alpha:]", ""),
         log_increase   = stri_replace_all_fixed(log_increase, " ", ""),
         log_increase   = as.numeric(log_increase) * 10,

         # apply calculations
         VL_RES_2       = case_when(
            VL_RES_2 == 99999 ~ 0,
            !is.na(log_increase) &
               is.na(log_multiplier) &
               !stri_detect_fixed(VL_RES, "X") &
               !stri_detect_fixed(VL_RES, "E") &
               !stri_detect_fixed(VL_RES, "^") ~ as.numeric(stri_replace_all_regex(log_raw, "[^[:digit:]]", "")) * log_increase,
            !is.na(log_increase) &
               !is.na(log_multiplier) &
               stri_detect_fixed(VL_RES, "E") &
               !stri_detect_fixed(VL_RES, "X") &
               !stri_detect_fixed(VL_RES, "^") ~ as.numeric(stri_replace_all_regex(log_raw, "[^[:digit:]]", "")) *
               log_increase *
               log_multiplier,
            is.na(VL_RES_2) &
               stri_count_fixed(VL_RES, ".") > 1 &
               StrIsNumeric(stri_replace_all_fixed(VL_RES, ".", "")) ~ as.numeric(stri_replace_all_regex(VL_RES, ".", "")),
            TRUE ~ VL_RES_2
         )
      ) %>%
      select(
         -VL_RES,
         -log_raw,
         -log_multiplier,
         -log_increase,
         -has_alpha,
         -less_than,
         -sci_cal
      ) %>%
      relocate(VL_RES_2, .after = !!vl_column_old) %>%
      rename(
         !!vl_column_new := VL_RES_2
      )

   return(data)
}