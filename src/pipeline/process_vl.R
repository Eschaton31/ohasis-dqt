process_vl <- function(data, result_old, result_new) {
   vl_column_old <- as.name(result_old)
   vl_column_new <- as.name(result_new)

   local_gs4_quiet()
   vl_corr <- read_sheet("1Yj-qP7sA8k-X0L9UHoXNl-TmBkPMfLkUIOlWNGJdENo", "vl_result", range = "A:B", col_types = "c") %>%
      mutate(VL_CORRECT = as.numeric(VL_CORRECT))

   data %<>%
      # clean results
      mutate(
         VL_RES = toupper(str_squish(!!vl_column_old)),
      ) %>%
      mutate_at(
         .vars = vars(VL_RES),
         ~str_replace_all(VL_RES, "\"", "") %>%
            str_replace_all("`", "") %>%
            str_replace_all(":", ".") %>%
            str_replace_all(" *- *", "-") %>%
            str_replace_all(" */ *", "/") %>%
            str_replace_all("\\.+", ".") %>%
            str_replace_all("\\*\\*\\*", "") %>%
            str_replace_all("LESSTHAN", "<") %>%
            str_replace_all("< +", "<") %>%
            str_replace_all("^> *", "") %>%
            # numbers
            str_replace_all("X10", " X 10") %>%
            str_squish() %>%
            str_replace_all("X 103", "EO3") %>%
            str_replace_all("X 106", "EO6") %>%
            str_replace_all("X 104", "EO4") %>%
            str_replace_all("X 10 *\\^4", "EO4") %>%
            str_replace_all("X 10 4", "EO4") %>%
            str_replace_all("X 10E3", "EO3") %>%
            str_replace_all("X 10E5", "EO5") %>%
            str_replace_all("X 10E5", "EO5") %>%
            str_replace_all("X 10 E4", "EO4") %>%
            str_replace_all("X 10 5", "EO5") %>%
            str_replace_all("X 105", "EO5") %>%
            str_replace_all("X 10\\(5\\)", "EO5") %>%
            str_replace_all("X 10\\(3\\)", "EO3") %>%
            str_replace_all("X 10$", "EO1") %>%
            str_replace_all("X 10 *\\(5TH POWER\\)", "EO5") %>%
            str_replace_all("10 TO THE 5TH", "EO5") %>%
            str_replace_all("X 10\\^([0-9]+)", "E\\1") %>%
            str_replace_all("X E05", "E05") %>%
            # hiv-1
            str_replace_all("\\bHIV 1\\b", "HIV-1") %>%
            str_replace_all("HIV-!", "HIV-1") %>%
            str_replace_all("HIV-1-", "HIV-1 ") %>%
            str_replace_all("HIV I ", "HIV-1 ") %>%
            # dates
            str_replace_all("[0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9]", "") %>%
            str_replace_all("[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]", "") %>%
            str_replace_all("[0-9][0-9]-[0-9][0-9]-[0-9][0-9]", "") %>%
            str_replace_all("[0-9][0-9]/[0-9][0-9]/[0-9][0-9]", "") %>%
            str_replace_all("[0-9]-[0-9][0-9]-[0-9][0-9]", "") %>%
            str_replace_all("[0-9]/[0-9][0-9]/[0-9][0-9]", "") %>%
            # undetectable
            str_replace_all("UNDECTED", "UNDETECTABLE") %>%
            str_replace_all("JNDETECTABLE", "UNDETECTABLE") %>%
            str_replace_all("UDETECTABLE", "UNDETECTABLE") %>%
            str_replace_all("\\bNO HIV\\b", "UNDETECTABLE") %>%
            str_replace_all("^NOT CET$", "UNDETECTABLE") %>%
            str_replace_all("^NOT DE$", "UNDETECTABLE") %>%
            str_replace_all("NOTDETECTED", "UNDETECTABLE") %>%
            str_replace_all("^NOR DETECTED$", "UNDETECTABLE") %>%
            str_replace_all("^NOT-DETECTED$", "UNDETECTABLE") %>%
            str_replace_all("NOTE DETECTED", "UNDETECTABLE") %>%
            str_replace_all("^TBD$", "UNDETECTABLE") %>%
            str_replace_all("^TMD$", "UNDETECTABLE") %>%
            str_replace_all("^TND$", "UNDETECTABLE") %>%
            str_replace_all("^RND$", "UNDETECTABLE") %>%
            str_replace_all("^TNTD$", "UNDETECTABLE") %>%
            str_replace_all("^N D$", "UNDETECTABLE") %>%
            str_replace_all("^MD$", "UNDETECTABLE") %>%
            str_replace_all("^HIV-1 NOT$", "UNDETECTABLE") %>%
            str_replace_all("^HIV\\+ND$", "UNDETECTABLE") %>%
            str_replace_all("^HIV\\+ND$", "UNDETECTABLE") %>%
            str_replace_all("^HIV\\+ND$", "UNDETECTABLE") %>%
            # detected
            str_replace_all("\\bETECTED\\b", "DETECTED") %>%
            str_replace_all("DETCTED", "DETECTED") %>%
            str_replace_all("DTECTED", "DETECTED") %>%
            str_replace_all("DETECTD", "DETECTED") %>%
            str_replace_all("DETECED", "DETECTED") %>%
            str_replace_all("DTETCTED", "DETECTED") %>%
            str_replace_all("DECTECTED", "DETECTED") %>%
            str_replace_all("DETECETED", "DETECTED") %>%
            str_replace_all("DETECTED\\+", "DETECTED") %>%
            str_replace_all("DFETECTED", "DETECTED") %>%
            str_replace_all("DETECTEDN", "DETECTED") %>%
            # not
            str_replace_all("N OT", "NOT") %>%
            str_replace_all("N0T", "NOT") %>%
            # copies
            str_replace_all("COPES", "COPIES") %>%
            str_replace_all("COPIEE", "COPIES") %>%
            str_replace_all("\\bCOPLIES\\b", "COPIES") %>%
            str_replace_all("\\bCOMPIES\\b", "COPIES") %>%
            str_replace_all("\\bCOPPIES\\b", "COPIES") %>%
            str_replace_all("\\bCOPIEZ\\b", "COPIES") %>%
            str_replace_all("\\bCPIES\\b", "COPIES") %>%
            str_replace_all("\\bCOPIS\\b", "COPIES") %>%
            str_replace_all("\\bCOPIE\\b", "COPIES") %>%
            str_replace_all("\\bCPIES\\b", "COPIES") %>%
            str_replace_all("\\bC0PIES\\b", "COPIES") %>%
            str_replace_all("\\bCPOIES\\b", "COPIES") %>%
            str_replace_all("\\bCOIES\\b", "COPIES") %>%
            str_replace_all("\\bCOIES\\b", "COPIES") %>%
            str_replace_all("\\bCOPES\\b", "COPIES") %>%
            str_replace_all("\\bXOPIES\\b", "COPIES") %>%
            str_replace_all("\\bC/", "COPIES/") %>%
            str_replace_all("CP/", "COPIES/") %>%
            str_replace_all(" *C/U$", "COPIES/UL") %>%
            str_replace_all(" *COPIES/M\\[", "COPIES/ML") %>%
            str_replace_all("/COPIES", "COPIES") %>%
            # results
            str_replace_all("HIV-1 VIRAL RNA DETECTED AT ", "") %>%
            str_replace_all("HIV0-1 DETECTED ", "") %>%
            str_replace_all("HIV-1 DETECTED ", "") %>%
            str_replace_all("HIV-1 DETECTED, ", "") %>%
            str_replace_all("HIV DETECTED, ", "") %>%
            str_replace_all("HIV-1 DETECETED, ", "") %>%
            str_replace_all("HIV-1 DETECTED,", "") %>%
            str_replace_all("HIV-DETECTED ", "") %>%
            str_replace_all("HIV 1DETECTED ", "") %>%
            str_replace_all("HOV-1 DETECTED ", "") %>%
            str_replace_all("HIV-1 DETECTED", "") %>%
            str_replace_all("HIV1 DETECTED ", "") %>%
            str_replace_all("HIV1 DETECTED", "") %>%
            str_replace_all("HIV DETECTED ", "") %>%
            str_replace_all("HIV DETECTED", "") %>%
            str_replace_all("HIVDETECTED ", "") %>%
            str_replace_all("HIVDETECTED", "") %>%
            str_replace_all("HI-1 DETECTED ", "") %>%
            str_replace_all("HIV-1 DETECETD, ", "") %>%
            str_replace_all("HIV 1- DETECTED ", "") %>%
            str_replace_all("HI-1 DETECTED", "") %>%
            str_replace_all("HIV- DETECTED ", "") %>%
            str_replace_all("HIV-1 NO DETECTED", "") %>%
            str_replace_all("HIV1-NOT DETECTED ", "") %>%
            str_replace_all("HIV-1DETECTED ", "") %>%
            str_replace_all("HIV 1 ", "") %>%
            str_replace_all("HIV-1 ", "") %>%
            str_replace_all(" *CELLS/UL", "") %>%
            str_replace_all(" *CELLS/ML", "") %>%
            str_replace_all(" *COPIES/ML", "") %>%
            str_replace_all(" *COPIES/UL", "") %>%
            str_replace_all(" COPIES", "") %>%
            str_replace_all("-COPIES", "") %>%
            str_replace_all("COPIES", "") %>%
            str_replace_all(" CPM", "") %>%
            str_replace_all(" PER ML$", "") %>%
            stri_replace_all_regex("^DETECTED ", "") %>%
            stri_replace_all_regex("^- ", "") %>%
            str_replace_all(" CELLS", "") %>%
            str_replace_all(", ", "") %>%
            str_replace_all(" ,", "") %>%
            str_replace_all(" `", "") %>%
            stri_replace_last_regex("\\.00$", "") %>%
            stri_replace_last_regex("([:digit:])EO", "$1E0") %>%
            str_replace_all("(12/11/2020) 2.51 E05 (LOG 5.40)", "2.51 E05 (LOG 5.40)") %>%
            str_replace_all(" *\\(\\)", ""),
      ) %>%
      mutate(
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
            condition = str_detect(VL_RES, "[:alpha:]"),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         scical         = case_when(
            stri_detect_fixed(VL_RES, "LOG") ~ "log",
            stri_detect_fixed(VL_RES, "^") ~ "exponent",
            stri_detect_fixed(VL_RES, "EO") ~ "eo",
            str_detect(VL_RES, "[:digit:]E *") ~ "eo",
            stri_detect_fixed(VL_RES, "X10") ~ "x10",
         ),
         scical_x10     = if_else(scical == "x10", VL_RES, NA_character_),
         scical_x10     = as.numeric(str_extract(scical_x10, "(.*)X10", 1)) * 10,
         scical_eo      = if_else(scical == "eo", VL_RES, NA_character_),
         scical_eo      = as.numeric(str_squish(str_extract(scical_eo, "(.+)E", 1))) * (10^as.numeric(str_extract(scical_eo, ".+E.+([0-9]+)", 1))),

         VL_RES_2       = case_when(
            scical == "x10" ~ scical_x10,
            scical == "eo" ~ scical_eo,
            less_than == 1 &
               stri_detect_fixed(VL_RES, "40") &
               stri_detect_fixed(VL_RES, "1.6") ~ 39,
            less_than == 1 & has_alpha == 0 ~ as.numeric(stri_replace_all_regex(VL_RES, "[^[:digit:]]", "")) - 1,
            less_than == 1 & has_alpha == 1 & is.na(scical) ~ as.numeric(stri_replace_all_regex(VL_RES, "[^[:digit:]]", "")) - 1,
            less_than == 0 &
               has_alpha == 1 &
               is.na(scical) &
               stri_detect_regex(VL_RES, "M$") ~ as.numeric(stri_replace_all_regex(VL_RES, "[^[:digit:]]", "")) * 1000000,
            StrIsNumeric(VL_RES) ~ as.numeric(VL_RES),
            stri_detect_fixed(VL_RES, ", ") & has_alpha == 0 ~ as.numeric(str_replace_all(VL_RES, ", ", "")),
            stri_detect_fixed(VL_RES, ",") & has_alpha == 0 ~ as.numeric(str_replace_all(VL_RES, ",", "")),
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
            !is.na(as.numeric(str_replace_all(VL_RES, " ", ""))) ~ as.numeric(str_replace_all(VL_RES, " ", ""))
         ),

         VL_ERRROR      = case_when(
            str_detect(VL_RES, "INVALID") ~ 1,
            str_detect(VL_RES, "ERROR") ~ 1,
            str_detect(VL_RES, "REPEAT") ~ 1,
            str_detect(VL_RES, "NOT SUFF") ~ 1,
            VL_RES == "20 TO 10,000,00" ~ 1,
            VL_RES == "CANNOT BE AMPLIIETD" ~ 1,
            VL_RES == "TNS" ~ 1,
            VL_RES == "QNS" ~ 1,
            VL_RES == "NS" ~ 1,
            TRUE ~ 0
         ),

         # tag for dropping
         VL_DROP        = case_when(
            VL_RES == " " ~ 1,
            VL_RES == "VL" ~ 1,
            VL_RES == "TO BE FOLLOW" ~ 1,
            VL_RES == "STILL FOR VL" ~ 1,
            VL_RES == "BASELINE VL" ~ 1,
            VL_RES == "NOT DONE" ~ 1,
            VL_RES == "NOT AVAILABLE" ~ 1,
            VL_RES == "PENDING" ~ 1,
            VL_RES == "WAITING" ~ 1,
            VL_RES == "TO SECURE" ~ 1,
            VL_RES == "NO RESULT" ~ 1,
            VL_RES == "NON-REACTIVE" ~ 1,
            VL_RES == "N/A" ~ 1,
            VL_RES == "NONE" ~ 1,
            VL_RES == "NO" ~ 1,
            VL_RES == "RC" ~ 1,
            VL_RES == "NR" ~ 1,
            VL_RES == "DONE.K.SILUNGAN" ~ 1,
            VL_RES == "MTB DETECTED" ~ 1,
            VL_RES == "CPPIES" ~ 1,
            VL_RES == "HIV I DETECTED" ~ 1,
            VL_RES == "HIV-1 RNA DETECTED" ~ 1,
            VL_RES == "RNA DETECTED" ~ 1,
            VL_RES == "DETECTED" ~ 1,
            VL_RES == "RIGHT UPPER LOBE FIBROTIC RESIDUALS" ~ 1,
            VL_RES == "20 TO 10,000,00" ~ 1,
            VL_RES == ",1.0EX07" ~ 1,
            VL_RES == "LOG 5.54" ~ 1,
            VL_RES == "LOG 1.14E05" ~ 1,
            VL_RES == "HIV-12-2021" ~ 1,
            VL_RES == "CANNOT BE AMPLIIETD" ~ 1,
            VL_RES == "12-SEP" ~ 1,
            VL_RES == "MONITORING" ~ 1,
            VL_RES == "NA" ~ 1,
            VL_RES == "O" ~ 1,
            VL_RES == "T" ~ 1,
            VL_RES == "." ~ 1,
            VL_RES == "*" ~ 1,
            VL_RES == "<" ~ 1,
            VL_RES == "C" ~ 1,
            VL_RES == "D" ~ 1,
            VL_RES == "H" ~ 1,
            VL_RES == "HI" ~ 1,
            VL_RES == "HIV" ~ 1,
            VL_RES == "TNS" ~ 1,
            VL_RES == "QNS" ~ 1,
            VL_RES == "NS" ~ 1,
            VL_RES == "IVM" ~ 1,
            VL_RES == "LAB" ~ 1,
            VL_RES == "N3" ~ 1,
            VL_RES == "@RITM" ~ 1,
            VL_RES == "EXTRACTED" ~ 1,
            VL_RES == "WLY21-0477" ~ 1,
            VL_RES == "NORMAL CHEST" ~ 1,
            VL_RES == "NORMAL CHEST" ~ 1,
            str_detect(VL_RES, "REPEAT") ~ 1,
            stri_detect_fixed(VL_RES, "RETRIEVING") ~ 1,
            stri_detect_fixed(VL_RES, "INVALID") ~ 1,
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
         log_increase   = str_replace_all(log_increase, "[:alpha:]", ""),
         log_increase   = str_replace_all(log_increase, " ", ""),
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
               StrIsNumeric(str_replace_all(VL_RES, ".", "")) ~ as.numeric(stri_replace_all_regex(VL_RES, ".", "")),
            TRUE ~ VL_RES_2
         )
      ) %>%
      left_join(vl_corr, join_by(VL_RES)) %>%
      mutate(
         VL_RES_2 = coalesce(VL_CORRECT, VL_RES_2)
      ) %>%
      select(
         -VL_RES,
         -log_raw,
         -log_multiplier,
         -log_increase,
         -has_alpha,
         -less_than,
         -starts_with("scical")
      ) %>%
      relocate(VL_RES_2, .after = !!vl_column_old) %>%
      rename(
         !!vl_column_new := VL_RES_2
      )

   return(data)
}