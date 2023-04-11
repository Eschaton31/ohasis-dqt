##  prepare dataset for tx indicators ------------------------------------------

merge_harp <- function(harp, coverage) {
   data <- harp$tx$new %>%
      rename(
         HARPTX_SEX = sex
      ) %>%
      # ML & RTT data
      left_join(
         y  = harp$tx$old %>%
            mutate(
               # tag based on pepfar Defintion
               outcome28 = hiv_tx_outcome(outcome, latest_nextpickup, coverage$prev$date, 28),
               onart28   = if_else(
                  condition = outcome28 == "alive on arv",
                  true      = 1,
                  false     = 0,
                  missing   = 0
               ),
            ) %>%
            select(
               art_id,
               ltfu_date    = latest_nextpickup,
               prev_onart28 = onart28
            ),
         by = join_by(art_id)
      ) %>%
      left_join(
         y  = harp$dx %>%
            select(
               idnum,
               transmit,
               sexhow,
               confirm_date,
               ref_report,
               HARPDX_BIRTHDATE  = bdate,
               HARPDX_SEX        = sex,
               HARPDX_SELF_IDENT = self_identity,
            ),
         by = join_by(idnum)
      )

   return(data)
}

tag_indicators <- function(data, coverage) {
   data %<>%
      mutate(
         # tag based on pepfar Defintion
         outcome28        = hiv_tx_outcome(outcome, latest_nextpickup, coverage$max, 28),
         onart28          = if_else(
            condition = outcome28 == "alive on arv",
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         vl_elig          = if_else(
            condition = (interval(artstart_date, coverage$max) / days(1)) > 92,
            true      = 1,
            false     = 0,
            missing   = 0
         ),

         # tag specific indicators
         TX_CURR          = if_else(
            condition = onart28 == 1,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         TX_NEW           = if_else(
            condition = artstart_date %within% interval(coverage$min, coverage$max),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         TX_ML            = if_else(
            condition = prev_onart28 == 1 & onart28 == 0,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         TX_RTT           = if_else(
            condition = prev_onart28 == 0 & onart28 == 1,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         TX_PVLS_ELIGIBLE = if_else(
            condition = onart28 == 1 & vl_elig == 1,
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         TX_PVLS          = if_else(
            condition = onart28 == 1 &
               is.na(baseline_vl) &
               !is.na(vlp12m),
            true      = 1,
            false     = 0,
            missing   = 0
         ),

      )

   return(data)
}

generate_disagg <- function(data, coverage) {
   data %<>%
      mutate(
         # sex variable (use registry if available)
         Sex             = coalesce(StrLeft(coalesce(HARPDX_SEX, HARPTX_SEX), 1), "(no data)"),

         # KAP
         msm             = case_when(
            Sex == "M" & sexhow %in% c("BISEXUAL", "HOMOSEXUAL") ~ 1,
            Sex == "M" & sexhow == "HETEROSEXUAL" ~ 0,
            TRUE ~ 0
         ),
         tgw             = if_else(
            condition = msm == 1 & HARPDX_SELF_IDENT %in% c("FEMALE", "OTHERS"),
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         hetero          = if_else(
            condition = sexhow == "HETEROSEXUAL",
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         pwid            = if_else(
            condition = transmit == "IVDU",
            true      = 1,
            false     = 0,
            missing   = 0
         ),
         unknown         = case_when(
            transmit == "UNKNOWN" ~ 1,
            is.na(transmit) ~ 1,
            TRUE ~ 0
         ),
         `KP Population` = case_when(
            msm == 1 & tgw == 0 ~ "MSM",
            msm == 1 & tgw == 1 ~ "TGW",
            pwid == 1 ~ "PWID",
            Sex == "F" ~ "(not included)",
            unknown == 1 ~ "(no data)",
            TRUE ~ "Non-MSM"
         ),

         # Age Band
         curr_age        = floor(curr_age),
         Age_Band        = case_when(
            curr_age >= 0 & curr_age < 5 ~ "01_0-4",
            curr_age >= 5 & curr_age < 10 ~ "02_5-9",
            curr_age >= 10 & curr_age < 15 ~ "03_10-14",
            curr_age >= 15 & curr_age < 20 ~ "04_15-19",
            curr_age >= 20 & curr_age < 25 ~ "05_20-24",
            curr_age >= 25 & curr_age < 30 ~ "06_25-29",
            curr_age >= 30 & curr_age < 35 ~ "07_30-34",
            curr_age >= 35 & curr_age < 40 ~ "08_35-39",
            curr_age >= 40 & curr_age < 45 ~ "09_40-44",
            curr_age >= 45 & curr_age < 50 ~ "10_45-49",
            curr_age >= 50 & curr_age < 55 ~ "11_50-54",
            curr_age >= 55 & curr_age < 60 ~ "12_55-59",
            curr_age >= 60 & curr_age < 65 ~ "13_60-64",
            curr_age >= 65 & curr_age < 1000 ~ "14_65+",
            TRUE ~ "99_(no data)"
         ),
         `DATIM Age`     = if_else(
            condition = curr_age < 15,
            true      = "<15",
            false     = ">=15",
            missing   = "(no data)"
         ),

         ffup_to_pickup  = floor(interval(latest_ffupdate, latest_nextpickup) / days(1)),
         tat_confirm_art = floor(interval(confirm_date, artstart_date) / days(1)),
         days_before_ml  = floor(interval(artstart_date, latest_nextpickup) / days(1)),
         days_before_rtt = floor(interval(ltfu_date, latest_ffupdate) / days(1)),
      ) %>%
      faci_code_to_id(
         ohasis$ref_faci_code %>% distinct(FACI_CODE, SUB_FACI_CODE, .keep_all = TRUE),
         c(FACI_ID = "realhub", SUB_FACI_ID = "realhub_branch")
      ) %>%
      left_join(
         y  = coverage$sites %>%
            select(
               FACI_ID,
               starts_with("site_")
            ) %>%
            distinct_all(),
         by = join_by(FACI_ID)
      ) %>%
      ohasis$get_faci(
         list(`Site/Organization` = c("FACI_ID", "SUB_FACI_ID")),
         "name",
         c("Site City", "Site Province", "Site Region")
      )

   return(data)
}

.init <- function(envir = parent.env(environment())) {
   p             <- envir
   p$linelist$tx <- merge_harp(p$harp, p$coverage) %>%
      tag_indicators(p$coverage) %>%
      generate_disagg(p$coverage)
}