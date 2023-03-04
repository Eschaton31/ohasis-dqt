if (!exists("dr"))
   dr <- new.env()

dr$`20220511-iit` <- new.env()

local(envir = dr$`20220511-iit`, {
   data <- list()

   for (i in c("2021", "2022")) {
      for (j in c("03", "06", "09", "12")) {
         ref_date       <- ceiling_date(as.Date(paste(sep = "-", i, j, "01")), unit = "month") - 1
         start_period   <- floor_date(ref_date %m-% months(3), unit = "days")
         start_onart    <- floor_date(start_period %m-% months(3), unit = "days")
         start_onart_30 <- start_period - 3
         # print(start_period)
         # cutoff_30dy <- ref_date - 30

         data[[glue("{i}{j}")]] <- read_dta(hs_data("harp_tx", "outcome", i, j)) %>%
            mutate_if(
               .predicate = is.character,
               ~str_squish(.) %>%
                  if_else(. == "", NA_character_, ., .)
            ) %>%
            mutate(
               diff    = floor(difftime(ref_date, latest_nextpickup, units = "days")),
               outcome = case_when(
                  outcome == "alive on arv" ~ "onart",
                  outcome == "lost to follow up" ~ "ltfu",
                  outcome == "trans out" ~ "transout",
                  TRUE ~ outcome
               )
            ) %>%
            mutate(
               .after       = outcome,
               outcome_30dy = case_when(
                  outcome == "dead" ~ "dead",
                  outcome == "stopped" ~ "stopped",
                  diff > 30 ~ "ltfu",
                  diff <= 30 ~ "onart",
                  outcome == "ltfu" ~ "ltfu",
                  TRUE ~ "(no data)"
               )
            ) %>%
            mutate(
               iit      = if_else(
                  (artstart_date >= start_period | latest_nextpickup > start_onart) & outcome != "onart",
                  floor(interval(artstart_date, latest_nextpickup) / days(1)),
                  as.numeric(NA),
                  as.numeric(NA)
               ),
               iit_30dy = if_else(
                  (artstart_date >= start_period | latest_nextpickup > start_onart_30) & outcome_30dy != "onart",
                  floor(interval(artstart_date, latest_nextpickup) / days(1)),
                  as.numeric(NA),
                  as.numeric(NA)
               ),
               age      = if (i == 2022) curr_age else age,
               hub      = toupper(hub)
            ) %>%
            mutate_at(
               .vars = vars(iit, iit_30dy),
               ~case_when(
                  . < 90 ~ "1) <3mos ontx",
                  . %in% seq(90, 179) ~ "2) 3-5mos ontx",
                  . >= 180 ~ "3) 6mos+ ontx",
               )
            )

         curr_names <- names(data[[glue("{i}{j}")]])
         if (!("txreg" %in% curr_names)) {

            data[[glue("{i}{j}")]] %<>%
               left_join(
                  y  = ohasis$ref_faci %>%
                     filter(SUB_FACI_ID == "", nchar(FACI_CODE) == 3) %>%
                     select(
                        hub    = FACI_CODE,
                        txreg  = FACI_NHSSS_REG,
                        txprov = FACI_NHSSS_PROV,
                        txmunc = FACI_NHSSS_MUNC,
                     ) %>%
                     mutate(
                        hub = case_when(
                           stri_detect_regex(hub, "^SAIL") ~ "SHP",
                           stri_detect_regex(hub, "^TLY") ~ "TLY",
                           TRUE ~ hub
                        ),
                     ) %>%
                     distinct(hub, .keep_all = TRUE),
                  by = "hub"
               ) %>%
               mutate(
                  txreg  = if (i == 2022) tx_reg else txreg,
                  txprov = if (i == 2022) tx_prov else txprov,
                  txmunc = if (i == 2022) tx_munc else txmunc,
               )
         }

         if (i >= 2022) {
            data[[glue("{i}{j}")]] %<>%
               left_join(
                  y  = hs_data("harp_tx", "reg", i, j) %>%
                     read_dta() %>%
                     mutate(
                        confirmatory_code = case_when(
                           confirmatory_code == "" & uic != "" ~ as.character(glue("*{uic}")),
                           confirmatory_code == "" & px_code != "" ~ as.character(glue("*{px_code}")),
                           art_id == 43460 ~ "JEJO0111221993_1",
                           art_id == 82604 ~ "JEJO0111221993_2",
                           TRUE ~ confirmatory_code
                        ),
                        sacclcode         = gsub("[^[:alnum:]]", "", confirmatory_code)
                     ) %>%
                     select(
                        art_id,
                        sacclcode
                     ),
                  by = "art_id"
               )
         }

         data[[glue("{i}{j}")]] %<>%
            select(
               any_of("art_id"),
               idnum,
               sex,
               # birthdate,
               age,
               artstart_date,
               sacclcode,
               "hub_{{i}}{{j}}"          := hub,
               "ffup_{{i}}{{j}}"         := latest_ffupdate,
               "pickup_{{i}}{{j}}"       := latest_nextpickup,
               "outcome_3mo_{{i}}{{j}}"  := outcome,
               "outcome_30dy_{{i}}{{j}}" := outcome_30dy,
               "iit_3mo_{{i}}{{j}}"      := iit,
               "iit_30dy_{{i}}{{j}}"     := iit_30dy,
               "txreg_{{i}}{{j}}"        := txreg,
               "txprov_{{i}}{{j}}"       := txprov,
               "txmunc_{{i}}{{j}}"       := txmunc,
               "age_{{i}}{{j}}"          := age
            ) %>%
            rename_all(
               ~stri_replace_all_fixed(., "\"", "")
            )
      }
   }
   rm(i, j)
})

df <- dr$`20220511-iit`$data$`202212` %>%
   select(
      art_id,
      idnum,
      sex,
      # birthdate,
      # age,
      artstart_date,
      sacclcode,
      hub_202212,
      ffup_202212,
      pickup_202212,
      outcome_3mo_202212,
      outcome_30dy_202212,
      iit_3mo_202212,
      iit_30dy_202212,
      txreg_202212,
      txprov_202212,
      txmunc_202212,
      age_202212
   )

for_merge   <- names(dr$`20220511-iit`$data)
for_merge   <- for_merge[for_merge != "202112"]
for_merge   <- sort(for_merge, decreasing = TRUE)
for_merge_1 <- c("202209", "202206", "202203")
for_merge_2 <- c("202112", "202109", "202106", "202103")

for (i in for_merge_1)
   df <- df %>%
      left_join(
         y  = dr$`20220511-iit`$data[[i]] %>%
            select(
               art_id,
               ends_with(i)
            ),
         by = "art_id"
      )

for (i in for_merge_2)
   df <- df %>%
      left_join(
         y  = dr$`20220511-iit`$data[[i]] %>%
            select(
               sacclcode,
               ends_with(i)
            ),
         by = "sacclcode"
      )

df %>%
   left_join(
      y  = read_dta(
         ohasis$get_data("harp_dx", "2021", "12"),
         col_select = c("idnum", "transmit", "sexhow", "region", "province", "muncity", "dxlab_standard", "confirmlab", "confirm_date")
      ),
      by = "idnum"
   )

write_dta(
   df %>%
      left_join(
         y  = read_dta(
            ohasis$get_data("harp_full", "2022", 12),
            col_select = c("idnum", "transmit", "sexhow", "region", "province", "muncity", "dxlab_standard", "confirmlab", "confirm_date", "self_identity")
         ),
         by = "idnum"
      ) %>%
      format_stata(),
   "C:/Users/Administrator/Downloads/20230202_iit_2021-2022_forAEM.dta"
)