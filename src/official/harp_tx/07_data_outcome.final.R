##  Append w/ old Registry -----------------------------------------------------

finalize_outcomes <- function(data) {
   data <- data %>%
      arrange(art_id) %>%
      mutate(
         hub               = if_else(
            condition = use_db == 1,
            true      = curr_hub,
            false     = prev_hub,
         ),
         branch            = if_else(
            condition = use_db == 1,
            true      = curr_branch,
            false     = prev_branch,
         ),
         sathub            = if_else(
            condition = use_db == 1,
            true      = curr_sathub,
            false     = prev_sathub,
         ),
         transhub          = if_else(
            condition = use_db == 1,
            true      = curr_transhub,
            false     = prev_transhub,
         ),
         realhub           = if_else(
            condition = use_db == 1,
            true      = curr_realhub,
            false     = prev_realhub,
         ),
         realhub_branch    = if_else(
            condition = use_db == 1,
            true      = curr_realhub_branch,
            false     = prev_realhub_branch,
         ),
         latest_ffupdate   = if_else(
            condition = use_db == 1,
            true      = curr_ffup,
            false     = prev_ffup,
         ),
         latest_nextpickup = if_else(
            condition = use_db == 1,
            true      = curr_pickup,
            false     = prev_pickup,
         ),
         latest_regimen    = if_else(
            condition = use_db == 1,
            true      = curr_regimen,
            false     = prev_regimen,
         ),
         art_reg           = if_else(
            condition = use_db == 1,
            true      = curr_artreg,
            false     = prev_artreg,
         ),
         line              = if_else(
            condition = use_db == 1,
            true      = curr_line,
            false     = prev_line,
         ),
         curr_outcome      = case_when(
            prev_outcome == "dead" ~ "dead",
            TRUE ~ curr_outcome
         ),
         newonart          = if_else(
            condition = year(artstart_date) == as.numeric(ohasis$yr) &
               month(artstart_date) == as.numeric(ohasis$mo),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         onart             = if_else(
            condition = curr_outcome == "alive on arv",
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         curr_class        = if_else(
            condition = is.na(curr_class),
            true      = prev_class,
            false     = curr_class
         ),
      )
   return(data)
}

finalize_faci <- function(data) {
   data %<>%
      select(
         -any_of(
            c(
               "tx_reg",
               "tx_prov",
               "tx_munc",
               "real_reg",
               "real_prov",
               "real_munc"
            )
         )
      ) %>%
      mutate(
         # finalize realhub data
         realhub_branch = if_else(
            condition = is.na(realhub),
            true      = branch,
            false     = realhub_branch
         ),
         realhub        = if_else(
            condition = is.na(realhub),
            true      = hub,
            false     = realhub
         )
      ) %>%
      mutate(
         branch         = case_when(
            hub == "TLY" & is.na(branch) ~ "TLY-ANGLO",
            TRUE ~ branch
         ),
         realhub_branch = case_when(
            realhub == "TLY" & is.na(realhub_branch) ~ "TLY-ANGLO",
            TRUE ~ realhub_branch
         ),
      ) %>%
      mutate_at(
         .vars = vars(hub, realhub),
         ~case_when(
            stri_detect_regex(., "^SAIL") ~ "SAIL",
            stri_detect_regex(., "^TLY") ~ "TLY",
            TRUE ~ .
         )
      ) %>%
      mutate(
         branch         = case_when(
            hub == "SHP" & is.na(branch) ~ "SHIP-MAKATI",
            hub == "TLY" & is.na(branch) ~ "TLY-ANGLO",
            TRUE ~ branch
         ),
         realhub_branch = case_when(
            realhub == "SHP" & is.na(realhub_branch) ~ "SHIP-MAKATI",
            realhub == "TLY" & is.na(realhub_branch) ~ "TLY-ANGLO",
            TRUE ~ realhub_branch
         ),
         realhub        = case_when(
            stri_detect_regex(realhub_branch, "^SAIL") ~ "SAIL",
            TRUE ~ realhub
         ),
      ) %>%
      faci_code_to_id(
         ohasis$ref_faci_code %>% distinct(FACI_CODE, SUB_FACI_CODE, .keep_all = TRUE),
         c(FACI_ID = "hub", SUB_FACI_ID = "branch")
      ) %>%
      left_join(
         y  = ohasis$ref_faci %>%
            select(
               FACI_ID,
               SUB_FACI_ID,
               tx_reg  = FACI_NHSSS_REG,
               tx_prov = FACI_NHSSS_PROV,
               tx_munc = FACI_NHSSS_MUNC,
            ),
         by = join_by(FACI_ID, SUB_FACI_ID)
      ) %>%
      select(-FACI_ID, -SUB_FACI_ID) %>%
      faci_code_to_id(
         ohasis$ref_faci_code %>% distinct(FACI_CODE, SUB_FACI_CODE, .keep_all = TRUE),
         c(FACI_ID = "realhub", SUB_FACI_ID = "realhub_branch")
      ) %>%
      left_join(
         y  = ohasis$ref_faci %>%
            select(
               FACI_ID,
               SUB_FACI_ID,
               real_reg  = FACI_NHSSS_REG,
               real_prov = FACI_NHSSS_PROV,
               real_munc = FACI_NHSSS_MUNC,
            ),
         by = join_by(FACI_ID, SUB_FACI_ID)
      ) %>%
      select(-FACI_ID, -SUB_FACI_ID) %>%
      select(
         REC_ID,
         CENTRAL_ID,
         art_id,
         idnum,
         prep_id,
         mort_id,
         sex,
         curr_age,
         hub,
         branch,
         sathub,
         transhub,
         tx_reg,
         tx_prov,
         tx_munc,
         realhub,
         realhub_branch,
         real_reg,
         real_prov,
         real_munc,
         artstart_date,
         class               = curr_class,
         outcome             = curr_outcome,
         latest_ffupdate,
         latest_nextpickup,
         latest_regimen,
         previous_ffupdate   = prev_ffup,
         previous_nextpickup = prev_pickup,
         previous_regimen    = prev_regimen,
         art_reg,
         line,
         newonart,
         onart
      ) %>%
      distinct_all() %>%
      arrange(art_id, desc(latest_nextpickup)) %>%
      distinct(art_id, .keep_all = TRUE) %>%
      mutate(central_id = CENTRAL_ID)

   return(data)
}

reg_disagg <- function(data, regimen_col, reg_disagg_col, reg_line_col) {
   concat_col     <- reg_disagg_col
   regimen_col    <- as.name(regimen_col)
   reg_disagg_col <- as.name(reg_disagg_col)
   reg_line_col   <- as.name(reg_line_col)

   data %<>%
      mutate(
         # reg disagg
         !!reg_disagg_col := toupper(str_squish(!!regimen_col)),
         r_abc            = if_else(
            stri_detect_fixed(!!reg_disagg_col, "ABC") &
               !stri_detect_fixed(!!reg_disagg_col, "ABCSYR"),
            "ABC",
            NA_character_
         ),
         r_abcsyr         = if_else(stri_detect_fixed(!!reg_disagg_col, "ABCSYR"), "ABCsyr", NA_character_),
         r_azt_3tc        = if_else(stri_detect_fixed(!!reg_disagg_col, "AZT/3TC"), "AZT/3TC", NA_character_),
         r_azt            = if_else(
            stri_detect_fixed(!!reg_disagg_col, "AZT") &
               !stri_detect_fixed(!!reg_disagg_col, "AZT/3TC") &
               !stri_detect_fixed(!!reg_disagg_col, "AZTSYR"),
            "AZT",
            NA_character_
         ),
         r_aztsyr         = if_else(stri_detect_fixed(!!reg_disagg_col, "AZTSYR"), "AZTsyr", NA_character_),
         r_tdf            = if_else(
            stri_detect_fixed(!!reg_disagg_col, "TDF") &
               !stri_detect_fixed(!!reg_disagg_col, "TDF/3TC") &
               !stri_detect_fixed(!!reg_disagg_col, "TDF100MG"),
            "TDF",
            NA_character_
         ),
         r_tdf_3tc        = if_else(
            stri_detect_fixed(!!reg_disagg_col, "TDF/3TC") &
               !stri_detect_fixed(!!reg_disagg_col, "TDF/3TC/EFV") &
               !stri_detect_fixed(!!reg_disagg_col, "TDF/3TC/DTG"),
            "TDF/3TC",
            NA_character_
         ),
         r_tdf_3tc_efv    = if_else(stri_detect_fixed(!!reg_disagg_col, "TDF/3TC/EFV"), "TDF/3TC/EFV", NA_character_),
         r_tdf_3tc_dtg    = if_else(stri_detect_fixed(!!reg_disagg_col, "TDF/3TC/DTG"), "TDF/3TC/DTG", NA_character_),
         r_tdf100         = if_else(stri_detect_fixed(!!reg_disagg_col, "TDF100MG"), "TDF100mg", NA_character_),
         r_xtc            = case_when(
            stri_detect_fixed(!!reg_disagg_col, "3TC") &
               !stri_detect_fixed(!!reg_disagg_col, "/3TC") &
               !stri_detect_fixed(!!reg_disagg_col, "3TCSYR") ~ "3TC",
            stri_detect_fixed(!!reg_disagg_col, "D4T/3TC") ~ "D4T/3TC",
            TRUE ~ NA_character_
         ),
         r_xtcsyr         = if_else(stri_detect_fixed(!!reg_disagg_col, "3TCSYR"), "3TCsyr", NA_character_),
         r_nvp            = if_else(
            stri_detect_fixed(!!reg_disagg_col, "NVP") &
               !stri_detect_fixed(!!reg_disagg_col, "NVPSYR"),
            "NVP",
            NA_character_
         ),
         r_nvpsyr         = if_else(stri_detect_fixed(!!reg_disagg_col, "NVPSYR"), "NVPsyr", NA_character_),
         r_efv            = if_else(
            stri_detect_fixed(!!reg_disagg_col, "EFV") &
               !stri_detect_fixed(!!reg_disagg_col, "/EFV") &
               !stri_detect_fixed(!!reg_disagg_col, "EFV50MG") &
               !stri_detect_fixed(!!reg_disagg_col, "EFV200MG") &
               !stri_detect_fixed(!!reg_disagg_col, "EFVSYR"),
            "EFV",
            NA_character_
         ),
         r_efv50          = if_else(stri_detect_fixed(!!reg_disagg_col, "EFV50MG"), "EFV50mg", NA_character_),
         r_efv200         = if_else(stri_detect_fixed(!!reg_disagg_col, "EFV200MG"), "EFV200mg", NA_character_),
         r_efvsyr         = if_else(stri_detect_fixed(!!reg_disagg_col, "EFVSYR"), "EFVsyr", NA_character_),
         r_dtg            = if_else(
            stri_detect_fixed(!!reg_disagg_col, "DTG") &
               !stri_detect_fixed(!!reg_disagg_col, "/DTG"),
            "DTG",
            NA_character_
         ),
         r_lpvr           = if_else(
            (stri_detect_fixed(!!reg_disagg_col, "LPV/R") |
               stri_detect_fixed(!!reg_disagg_col, "LPVR")) &
               (!stri_detect_fixed(!!reg_disagg_col, "RSYR") &
                  !stri_detect_fixed(!!reg_disagg_col, "R PEDIA")),
            "LPV/r",
            NA_character_
         ),
         r_lpvr_pedia     = if_else(
            stri_detect_fixed(!!reg_disagg_col, "LPV") &
               (stri_detect_fixed(!!reg_disagg_col, "RSYR") |
                  stri_detect_fixed(!!reg_disagg_col, "R PEDIA")),
            "LPV/rsyr",
            NA_character_
         ),
         r_ril            = if_else(stri_detect_fixed(!!reg_disagg_col, "RIL"), "RIL", NA_character_),
         r_ral            = if_else(stri_detect_fixed(!!reg_disagg_col, "RAL"), "RAL", NA_character_),
         r_ftc            = if_else(stri_detect_fixed(!!reg_disagg_col, "FTC"), "FTC", NA_character_),
         r_idv            = if_else(stri_detect_fixed(!!reg_disagg_col, "IDV"), "IDV", NA_character_)
      ) %>%
      unite(
         col   = concat_col,
         sep   = '+',
         na.rm = T,
         starts_with("r_", ignore.case = FALSE)
      ) %>%
      mutate(
         # reg disagg
         !!reg_line_col := case_when(
            !!reg_disagg_col == "AZT/3TC+NVP" ~ 1,
            !!reg_disagg_col == "AZT+3TC+NVP" ~ 1,
            !!reg_disagg_col == "AZT/3TC+EFV" ~ 1,
            !!reg_disagg_col == "AZT+3TC+EFV" ~ 1,
            !!reg_disagg_col == "TDF/3TC/EFV" ~ 1,
            !!reg_disagg_col == "TDF/3TC+EFV" ~ 1,
            !!reg_disagg_col == "TDF+3TC+EFV" ~ 1,
            !!reg_disagg_col == "TDF/3TC+NVP" ~ 1,
            !!reg_disagg_col == "TDF+3TC+NVP" ~ 1,
            !!reg_disagg_col == "ABC+3TC+NVP" ~ 1,
            !!reg_disagg_col == "ABC+3TC+EFV" ~ 1,
            !!reg_disagg_col == "ABC+3TC+DTG" ~ 1,
            !!reg_disagg_col == "ABC+3TC+RTV" ~ 1,
            !!reg_disagg_col == "TDF+3TC+RTV" ~ 1,
            !!reg_disagg_col == "TDF/3TC+RTV" ~ 1,
            !!reg_disagg_col == "AZT/3TC+RTV" ~ 1,
            !!reg_disagg_col == "AZT+3TC+RTV" ~ 1,
            !!reg_disagg_col == "TDF/3TC/DTG" ~ 1,
            !!reg_disagg_col == "TDF/3TC+DTG" ~ 1,
            !!reg_disagg_col == "TDF+3TC+DTG" ~ 1,
            !!reg_disagg_col == "AZT/3TC+LPV/r" ~ 2,
            !!reg_disagg_col == "AZT+3TC+LPV/r" ~ 2,
            !!reg_disagg_col == "TDF/3TC+LPV/r" ~ 2,
            !!reg_disagg_col == "TDF+3TC+LPV/r" ~ 2,
            !!reg_disagg_col == "ABC+3TC+LPV/r" ~ 2,
            !!reg_disagg_col == "AZT+3TC+DTG" ~ 2,
            !!reg_disagg_col == "AZT/3TC+DTG" ~ 2,
            !!reg_disagg_col == "ABC+3TC+LPV/r" ~ 2,
            !!reg_disagg_col == "AZT/3TC+NVPsyr" ~ 2,
            !stri_detect_fixed(!!reg_disagg_col, "syr") & !stri_detect_fixed(!!reg_disagg_col, "pedia") ~ 3,
            stri_detect_fixed(!!reg_disagg_col, "syr") | stri_detect_fixed(!!reg_disagg_col, "pedia") ~ 4
         )
      )
   return(data)
}

get_reg_disagg <- function(data, col) {
   data %<>%
      # retag regimen
      select(-art_reg, -line) %>%
      mutate(
         arv_reg = stri_trans_tolower(!!as.name(col)),
         azt1    = if_else(stri_detect_fixed(arv_reg, "azt"), "azt", NA_character_),
         tdf1    = if_else(stri_detect_fixed(arv_reg, "tdf"), "tdf", NA_character_),
         d4t1    = if_else(stri_detect_fixed(arv_reg, "d4t"), "d4t", NA_character_),
         xtc1    = if_else(stri_detect_fixed(arv_reg, "3tc"), "3tc", NA_character_),
         efv1    = if_else(stri_detect_fixed(arv_reg, "efv"), "efv", NA_character_),
         nvp1    = if_else(stri_detect_fixed(arv_reg, "nvp"), "nvp", NA_character_),
         lpvr1   = if_else(stri_detect_fixed(arv_reg, "lpv/r"), "lpv/r", NA_character_),
         lpvr1   = if_else(stri_detect_fixed(arv_reg, "lpvr"), "lpvr", lpvr1),
         ind1    = if_else(stri_detect_fixed(arv_reg, "ind"), "ind", NA_character_),
         ral1    = if_else(stri_detect_fixed(arv_reg, "ral"), "ral", NA_character_),
         abc1    = if_else(stri_detect_fixed(arv_reg, "abc"), "abc", NA_character_),
         ril1    = if_else(stri_detect_fixed(arv_reg, "ril"), "ril", NA_character_),
         dtg1    = if_else(stri_detect_fixed(arv_reg, "dtg"), "dtg", NA_character_),
      ) %>%
      unite(
         col   = 'art_reg',
         sep   = ' ',
         na.rm = T,
         azt1,
         tdf1,
         d4t1,
         xtc1,
         efv1,
         nvp1,
         lpvr1,
         ind1,
         ral1,
         abc1,
         ril1,
         dtg1
      ) %>%
      mutate(
         # old line
         line = case_when(
            art_reg == "tdf 3tc efv" ~ 1,
            art_reg == "tdf 3tc nvp" ~ 1,
            art_reg == "azt 3tc efv" ~ 1,
            art_reg == "azt 3tc nvp" ~ 1,
            art_reg == "d4t 3tc efv" ~ 1,
            art_reg == "d4t 3tc nvp" ~ 1,
            art_reg == "tdf 3tc dtg" ~ 1,
            art_reg == "3tc nvp abc" ~ 1,
            art_reg == "3tc efv abc" ~ 1,
            art_reg == "3tc abc ril" ~ 1,
            art_reg == "tdf 3tc ril" ~ 1,
            art_reg == "azt 3tc ril" ~ 1,
            art_reg == "azt 3tc lpv/r" ~ 2,
            art_reg == "tdf 3tc lpv/r" ~ 2,
            art_reg == "d4t 3tc lpv/r" ~ 2,
            art_reg == "d4t 3tc idv" ~ 2,
            art_reg == "azt 3tc idv" ~ 2,
            art_reg == "tdf 3tc idv" ~ 2,
            art_reg == "3tc lpv/r abc" ~ 2,
            !is.na(art_reg) ~ 3
         ),

         # new line
         line = case_when(
            art_reg == "azt 3tc nvp" ~ 1,
            art_reg == "azt 3tc efv" ~ 1,
            art_reg == "tdf 3tc efv" ~ 1,
            art_reg == "tdf 3tc nvp" ~ 1,
            art_reg == "abc 3tc nvp" ~ 1,
            art_reg == "abc 3tc efv" ~ 1,
            art_reg == "abc 3tc dtg" ~ 1,
            art_reg == "abc 3tc rtv" ~ 1,
            art_reg == "tdf 3tc rtv" ~ 1,
            art_reg == "azt 3tc rtv" ~ 1,
            art_reg == "tdf 3tc dtg" ~ 1,
            art_reg == "azt 3tc lpv/r" ~ 2,
            art_reg == "tdf 3tc lpv/r" ~ 2,
            art_reg == "abc 3tc lpv/r" ~ 2,
            art_reg == "azt 3tc dtg" ~ 2,
            !is.na(art_reg) ~ 3
         ),
      ) %>%
      select(-central_id)
   return(data)
}

get_reg_order <- function(data, col) {
   regimen_col <- as.name(col)

   arvs <- data %>%
      select(art_id, arv_reg = !!regimen_col) %>%
      mutate(
         arv_order = case_when(
            arv_reg == "TDF/3TC/EFV" ~ 1,
            arv_reg == "TDF/3TC/DTG" ~ 2,
            arv_reg == "AZT/3TC/NVP" ~ 3,
            arv_reg == "AZT/3TC" ~ 4,
            arv_reg == "TDF/3TC" ~ 5,
            arv_reg == "TDF/FTC" ~ 6,
            arv_reg == "3TC/d4T" ~ 7,
            arv_reg == "AZT" ~ 8,
            arv_reg == "AZTsyr" ~ 9,
            arv_reg == "TDF" ~ 10,
            arv_reg == "TDFsyr" ~ 11,
            arv_reg == "3TC" ~ 12,
            arv_reg == "3TCsyr" ~ 13,
            arv_reg == "FTC" ~ 14,
            arv_reg == "EFV" ~ 15,
            arv_reg == "EFVsyr" ~ 16,
            arv_reg == "DTG" ~ 17,
            arv_reg == "NVP" ~ 18,
            arv_reg == "NVPsyr" ~ 19,
            arv_reg == "LPV/r" ~ 20,
            arv_reg == "LPV/rsyr" ~ 21,
            arv_reg == "IND" ~ 22,
            arv_reg == "RAL" ~ 23,
            arv_reg == "ABC" ~ 24,
            arv_reg == "ABCsyr" ~ 25,
            arv_reg == "RIL" ~ 26,
            arv_reg == "TAF" ~ 27,
            TRUE ~ 9999
         )
      ) %>%
      arrange(arv_order) %>%
      group_by(art_id) %>%
      summarise(latest_regimen = stri_c(arv_reg, collapse = "+")) %>%
      ungroup()

   data %<>%
      select(-!!regimen_col) %>%
      left_join(
         y  = arvs,
         by = join_by(art_id)
      )

   return(data)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(data) {
   check  <- list()
   update <- input(
      prompt  = "Run `outcome.final` validations?",
      options = c("1" = "yes", "2" = "no"),
      default = "1"
   )
   update <- substr(toupper(update), 1, 1)

   if (update == "1") {
      data %<>%
         left_join(
            y  = .GlobalEnv$nhsss$harp_tx$official$new_reg %>%
               select(
                  art_id,
                  confirmatory_code,
                  uic,
                  px_code,
                  philhealth_no,
                  philsys_id,
                  first,
                  middle,
                  last,
                  suffix,
                  birthdate
               ),
            by = "art_id"
         ) %>%
         relocate(
            .after = art_id,
            confirmatory_code,
            uic,
            px_code,
            philhealth_no,
            philsys_id,
            first,
            middle,
            last,
            suffix,
            birthdate
         )

      # special checks
      .log_info("Checking for shift out of TLD.")
      check[["tld to lte"]] <- data %>%
         filter(
            previous_regimen == "TDF/3TC/DTG" & latest_regimen == "TDF/3TC/EFV"
         )

      .log_info("Checking for no reg_line.")
      check[["missing reg_line"]] <- data %>%
         filter(
            is.na(reg_line)
         )

      .log_info("Checking for line 3.")
      check[["line 3"]] <- data %>%
         filter(
            reg_line == 3,
            onart == 1
         )

      .log_info("Checking for line 4.")
      check[["line 4"]] <- data %>%
         filter(
            reg_line == 4,
            onart == 1
         )

      .log_info("Checking for 2in1+1 and 1+1+1.")
      check[["2in1+1 and 1+1+1"]] <- data %>%
         filter(
            art_reg %in% c("tdf 3tc efv", "tdf 3tc dtg") & stri_detect_fixed(regimen, "+"),
            # regimen %in% c("TDF/3TC+DTG", "TDF/3TC+EFV", "TDF+3TC+DTG", "TDF+3TC+EFV"),
            # stri_detect_fixed(regimen, "TDF/3TC+DTG") |
            #    stri_detect_fixed(regimen, "TDF/3TC+EFV") |
            #    stri_detect_fixed(regimen, "TDF+3TC+DTG") |
            #    stri_detect_fixed(regimen, "TDF+3TC+EFV"),
            onart == 1
         )
   }
   return(check)
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      data <- read_rds(file.path(wd, "outcome.converted.RDS"))
      data <- finalize_outcomes(data) %>%
         finalize_faci() %>%
         get_reg_disagg("latest_regimen") %>%
         get_reg_order("latest_regimen")

      data <- reg_disagg(data, "latest_regimen", "regimen", "reg_line")
      data <- reg_disagg(data, "latest_regimen", "latest_regdisagg", "latest_regline")
      data <- reg_disagg(data, "previous_regimen", "previous_regdisagg", "previous_regline")

      .GlobalEnv$nhsss$harp_tx$official$new_outcome <- data

      check <- get_checks(data)
   })

   local(envir = .GlobalEnv, {
      ss <- flow_validation(nhsss$harp_tx, "outcome.final", ohasis$ym)
      for (sheet in c("2in1+1 and 1+1+1", "line 3", "line 4", "tld to lte")) {
         range_write_color(ss, sheet, "AB", "#FFF2CC")
         range_write_color(ss, sheet, "Y", "#d0e0e3")
         range_write_color(ss, sheet, "AI", "#d9ead3")
      }
      if ("new_outcome" %in% names(nhsss$harp_tx$corr))
         data <- .cleaning_list(data, nhsss$harp_tx$corr$new_outcome, "CENTRAL_ID", "character")

      rm(ss, sheet)
   })
}
