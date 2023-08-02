##  Set coverage ---------------------------------------------------------------

set_coverage <- function(max = end_ym(ohasis$yr, ohasis$mo)) {
   params   <- list()
   max_date <- as.Date(max)

   params$yr  <- year(max_date)
   params$mo  <- month(max_date)
   params$min <- start_ym(params$yr, params$mo)
   params$max <- max

   yrs <- as.character(seq(2020, params$yr))
   mos <- rep(12, length(yrs) - 1)
   mos <- append(mos, str_pad(params$mo, 2, "left", "0"))

   params$periods <- end_ym(yrs, mos)
   params$refs    <- psgc_aem(ohasis$ref_addr)

   return(params)
}

##  Get HARP datasets ----------------------------------------------------------

get_harp <- function(params) {
   harp <- list()

   # internal function to fix art columns
   fix_art_cols <- function(data) {
      data %<>%
         mutate(
            final_hub    = if ("realhub" %in% names(.)) realhub else toupper(hub),
            final_branch = if ("realhub" %in% names(.)) realhub_branch else NA_character_,
            baseline_vl  = na_if(baseline_vl, is.na(vl_date)),
         ) %>%
         mutate(
            .after      = outcome,
            outcome_old = hiv_tx_outcome(outcome, latest_nextpickup, end_period, 3, "months"),
            onart       = if_else(outcome_old == "alive on arv" | onart == 1, 1, 0, 0),
         ) %>%
         select(-outcome) %>%
         rename(outcome = outcome_old)

      return(data)
   }

   # read harp diagnosed clients with outcome
   harp$dx        <- lapply(params$periods, function(end_period) {
      ref_date <- str_split(end_period, "-")[[1]]
      ref_yr   <- ref_date[[1]]
      ref_mo   <- ref_date[[2]]

      data <- hs_data("harp_full", "reg", ref_yr, ref_mo) %>%
         read_dta(
            col_select = c(
               idnum,
               transmit,
               any_of(c("labcode", "labcode2", "class", "class2022", "gender_identity")),
               sexhow,
               age,
               cur_age,
               region,
               province,
               muncity,
               rhivda_done,
               confirmlab,
               dxlab_standard,
               dx_region,
               dx_province,
               dx_muncity,
               any_of(c("blood_extract_date", "specimen_receipt_date", "test_date", "t0_date", "visit_date", "confirm_date")),
               contains("hub"),
               contains("branch"),
               contains("vl"),
               latest_ffupdate,
               latest_nextpickup,
               latest_regimen,
               outcome,
               onart
            )
         ) %>%
         mutate_if(
            .predicate = is.character,
            ~na_if(str_squish(.), "")
         ) %>%
         mutate(
            end_period = end_period,
            vl_naive   = 0
         )

      if (as.numeric(ref_yr) >= 2022) {
         vl_naive <- hs_data("harp_vl", "naive_dx", ref_yr, ref_mo) %>%
            read_dta(
               col_select = c(
                  idnum,
                  vl_naive,
                  vl_date_first,
                  vl_result_first,
                  vl_date_last,
                  vl_result_last
               )
            )

         data %<>%
            select(-any_of("vl_naive")) %>%
            left_join(vl_naive, join_by(idnum)) %>%
            mutate(
               reactive_date = coalesce(blood_extract_date, specimen_receipt_date, test_date, t0_date, visit_date, confirm_date) %>% as.Date()
            )
      }

      return(data)
   })
   names(harp$dx) <- as.character(year(params$periods))

   # read harp diagnosed clients with outcome
   harp$tx        <- lapply(params$periods, function(end_period) {
      ref_date <- str_split(end_period, "-")[[1]]
      ref_yr   <- ref_date[[1]]
      ref_mo   <- ref_date[[2]]

      data <- hs_data("harp_tx", "outcome", ref_yr, ref_mo) %>%
         read_dta(
            col_select = c(
               any_of(c("art_id", "sacclcode", "saccl")),
               idnum,
               sex,
               any_of(c("birthdate", "curr_age", "cur_age")),
               contains("hub"),
               contains("branch"),
               contains("vl"),
               latest_ffupdate,
               latest_nextpickup,
               latest_regimen,
               outcome,
               onart
            )
         ) %>%
         mutate_if(
            .predicate = is.character,
            ~na_if(str_squish(.), "")
         ) %>%
         mutate(
            end_period = end_period,
            vl_naive   = 0
         )

      if (as.numeric(ref_yr) >= 2022) {
         art_reg <- hs_data("harp_tx", "reg", ref_yr, ref_mo) %>%
            read_dta(
               col_select = c(
                  art_id,
                  uic,
                  px_code,
                  confirmatory_code,
                  birthdate
               )
            ) %>%
            mutate(
               sacclcode = case_when(
                  confirmatory_code == "" & uic != "" ~ as.character(glue("*{uic}")),
                  confirmatory_code == "" & px_code != "" ~ as.character(glue("*{px_code}")),
                  art_id == 43460 ~ "JEJO0111221993_1",
                  art_id == 82604 ~ "JEJO0111221993_2",
                  TRUE ~ confirmatory_code
               ),
            ) %>%
            select(-uic, -px_code, -confirmatory_code)

         vl_naive <- hs_data("harp_vl", "naive_tx", ref_yr, ref_mo) %>%
            read_dta(
               col_select = c(
                  art_id,
                  vl_naive,
                  vl_date_first,
                  vl_result_first,
                  vl_date_last,
                  vl_result_last
               )
            )

         data %<>%
            select(-any_of("vl_naive")) %>%
            left_join(art_reg, join_by(art_id)) %>%
            left_join(vl_naive, join_by(art_id))
      }

      return(data)
   })
   names(harp$tx) <- as.character(year(params$periods))

   harp$dx <- lapply(harp$dx, fix_art_cols)
   harp$tx <- lapply(harp$tx, fix_art_cols)

   if ("2020" %in% names(harp$dx)) {
      harp$dx$`2020` %<>%
         left_join(
            y  = harp$dx[[length(harp$dx)]] %>%
               select(
                  idnum,
                  gender_identity,
                  class2022,
                  new_lab  = dxlab_standard,
                  new_reg  = dx_region,
                  new_prov = dx_province,
                  new_munc = dx_muncity
               ),
            by = join_by(idnum)
         ) %>%
         mutate(
            class2022       = coalesce(class2022, class),
            gender_identity = coalesce(gender_identity, "Unknown"),
            use_new         = case_when(
               dx_region == "UNKNOWN" & !is.na(new_reg) ~ 1,
               is.na(dx_region) & !is.na(new_reg) ~ 1,
               is.na(dx_province) & !is.na(new_prov) ~ 1,
               is.na(dx_muncity) & !is.na(new_munc) ~ 1,
               TRUE ~ 0
            ),
            dx_region       = if_else(use_new == 1, new_reg, dx_region, dx_region),
            dx_province     = if_else(use_new == 1, new_prov, dx_province, dx_province),
            dx_muncity      = if_else(use_new == 1, new_munc, dx_muncity, dx_muncity),
         ) %>%
         select(
            -new_reg,
            -new_prov,
            -new_munc,
         )
   }

   if ("2021" %in% names(harp$dx)) {
      harp$dx$`2021` %<>%
         left_join(
            y  = harp$dx$`2022` %>%
               select(
                  idnum,
                  class2022,
                  gender_identity,
               ),
            by = "idnum"
         ) %>%
         mutate(
            class2022       = coalesce(class2022, class),
            gender_identity = coalesce(gender_identity, "Unknown"),
         )
   }

   return(harp)
}

.init <- function(envir = parent.env(environment()), ...) {
   p    <- envir
   vars <- as.list(list(...))

   p$params <- set_coverage(vars$end_date)
   p$harp   <- get_harp(p$params)
}