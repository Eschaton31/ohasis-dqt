# confirmatory & testing columns joined onto form_hts / form_a
HTS_LAKE_COLS <- c(
   'confirm.confirm_faci',
   'confirm.confirm_sub_faci',
   'confirm.confirm_type',
   'confirm.confirm_code',
   'confirm.specimen_refer_type',
   'confirm.specimen_source',
   'confirm.specimen_sub_source',
   'confirm.date_collect',
   'confirm.date_receive',
   'confirm.confirm_result',
   'confirm.confirm_remarks',
   'confirm.signatory_1',
   'confirm.signatory_2',
   'confirm.signatory_3',
   'confirm.date_release',
   'confirm.date_confirm',
   'confirm.idnum',
   'confirm.rt_agreed',
   'confirm.rt_date',
   'confirm.rt_result',
   'confirm.rt_kit',
   'confirm.rt_vl_requested',
   'confirm.rt_vl_done',
   'confirm.rt_vl_date',
   'confirm.rt_vl_result',
   'confirm.rita_result',
   'test.t1_date',
   'test.t1_result',
   'test.t1_kit',
   'test.t2_date',
   'test.t2_result',
   'test.t2_kit',
   'test.t3_date',
   'test.t3_result',
   'test.t3_kit'
)

# base query for a form table; form_hts & form_a also get the confirmatory & testing columns
form_query <- function(con, form) {
   query <- QB$new(con)
   query$from(stri_c("ohasis_warehouse.", form))
   if (form != "form_cfbs") {
      do.call(query$select, as.list(c(stri_c(form, ".*"), HTS_LAKE_COLS)))
      query$leftJoin("ohasis_lake.px_hiv_confirmatory as confirm", stri_c(form, ".rec_id"), "=", "confirm.rec_id")
      query$leftJoin("ohasis_lake.px_hiv_testing as test", stri_c(form, ".rec_id"), "=", "test.rec_id")
   }
   return(query)
}

get_hts <- function(min, max, faci_ids = NULL) {
   con <- connect('mariadb-lw')
   on.exit(dbDisconnect(con), add = TRUE)

   read_forms <- function(min, max) {
      date_cols <- list(
         form_hts  = c('record_date', 'date_confirm', 'test.t0_date', 'test.t1_date', 'test.t2_date', 'test.t3_date'),
         form_a    = c('record_date', 'date_confirm', 'test.t0_date', 'test.t1_date', 'test.t2_date', 'test.t3_date'),
         form_cfbs = c('record_date', 'test_date')
      )

      forms <- lapply(names(date_cols), function(form) {
         query <- form_query(con, form)
         query$where(function(query = QB$new(con)) {
            for (col in date_cols[[form]]) {
               query$whereBetween(col, c(min, max), "or")
            }
            query$whereNested
         })

         if (!is.null(faci_ids)) {
            query$where(function(query = QB$new(con)) {
               query$whereIn("faci_id", faci_ids, 'or')
               query$whereIn("service_faci", faci_ids, 'or')
               query$whereNested
            })
         }

         query$get(format = 'Arrow')
      })

      return(setNames(forms, c("hts", "a", "cfbs")))
   }

   starts <- seq(as.Date(min), as.Date(max), by = "1 quarter")
   ends   <- c(starts[-1] - 1, as.Date(max))

   hts <- purrr::map2(as.character(starts), as.character(ends), function(start, end) {
      log_info(r"({green(start)} to {green(end)})")
      return(read_forms(start, end))
   })

   forms <- purrr::map(
      purrr::transpose(hts),
      ~bind_rows(.x) %>% distinct(rec_id, .keep_all = TRUE)
   )

   return(forms[c("hts", "a", "cfbs")])
}

get_hts_recs <- function(rec_ids) {
   con <- connect('mariadb-lw')
   on.exit(dbDisconnect(con), add = TRUE)

   forms <- lapply(c("form_hts", "form_a", "form_cfbs"), function(form) {
      query <- form_query(con, form)
      query$whereIn(stri_c(form, '.rec_id'), rec_ids)
      query$get(format = 'Arrow')
   })

   return(setNames(forms, c("hts", "a", "cfbs")))
}

# whole months between `date` and `ref`
months_since <- function(date, ref) {
   floor(interval(date, ref) / months(1))
}

# bucket a month count into recency categories
bucket_recency <- function(months) {
   case_when(
      months <= 1 ~ "p01m",
      months <= 3 ~ "p03m",
      months <= 6 ~ "p06m",
      months <= 12 ~ "p12m",
      months > 12 ~ "beyond_p12m",
      TRUE ~ "(no data)"
   )
}

# take the most recent of two recency categories
combine_recency <- function(x, y) {
   case_when(
      x == "p01m" | y == "p01m" ~ "p01m",
      x == "p03m" | y == "p03m" ~ "p03m",
      x == "p06m" | y == "p06m" ~ "p06m",
      x == "p12m" | y == "p12m" ~ "p12m",
      x == "beyond_p12m" | y == "beyond_p12m" ~ "beyond_p12m",
      x == "none" | y == "none" ~ "none",
      x == "(no data)" & y == "(no data)" ~ "(no data)",
   )
}

# mode of transmission tagging, shared by process_hts() and the harp_dx registry (tag_mot step)
# male / female : expressions that flag the sex of the client
# requires expose_* coded as integers, p10y, perm_prov (10-digit psgc), age & risk_* columns
generate_mot <- function(data, male, female) {
   cebu <- c("07022", "07306", "07311", "07313")

   data %<>%
      mutate(
         .male      = coalesce({{male}}, FALSE),
         .female    = coalesce({{female}}, FALSE),

         # for mot
         motherisi1 = case_when(
            expose_hiv_mother > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithf   = case_when(
            expose_sex_f > 0 ~ 1,                      # HTS Form
            !is.na(expose_sex_f_av_date) ~ 1,          # HTS Form
            !is.na(expose_sex_f_av_nocondom_date) ~ 1, # HTS Form
            expose_sex_f_nocondom > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithm   = case_when(
            expose_sex_m > 0 ~ 1,                      # HTS Form
            !is.na(expose_sex_m_av_date) ~ 1,          # HTS Form
            !is.na(expose_sex_m_av_nocondom_date) ~ 1, # HTS Form
            expose_sex_m_nocondom > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithpro = case_when(
            expose_sex_paying > 0 ~ 1,
            TRUE ~ 0
         ),
         regularlya = case_when(
            expose_sex_payment > 0 ~ 1,
            TRUE ~ 0
         ),
         injectdrug = case_when(
            expose_drug_inject > 0 ~ 1,
            TRUE ~ 0
         ),
         chemsex    = case_when(
            expose_sex_drugs > 0 ~ 1, # HTS Form
            TRUE ~ 0
         ),
         receivedbt = case_when(
            expose_blood_transfuse > 0 ~ 1,
            TRUE ~ 0
         ),
         sti        = case_when(
            expose_sti > 0 ~ 1,
            TRUE ~ 0
         ),
         needlepri1 = case_when(
            expose_occupation > 0 ~ 1,
            TRUE ~ 0
         ),

         mot        = 0,
         # m->m only
         mot        = case_when(
            .male & expose_sex_m_nocondom == 1 ~ 1,
            .male & yr_last_m >= p10y ~ 1,
            .male & year(expose_sex_m_av_date) >= p10y ~ 1,          # HTS Form
            .male & year(expose_sex_m_av_nocondom_date) >= p10y ~ 1, # HTS Form
            TRUE ~ mot
         ),

         # m->m+f
         mot        = case_when(
            mot == 1 & expose_sex_f_nocondom == 1 ~ 2,
            mot == 1 & yr_last_f >= p10y ~ 2,
            mot == 1 & year(expose_sex_f_av_date) >= p10y ~ 2,          # HTS Form
            mot == 1 & year(expose_sex_f_av_nocondom_date) >= p10y ~ 2, # HTS Form
            TRUE ~ mot
         ),

         # m->f only
         mot        = case_when(
            .male & mot == 0 & expose_sex_f_nocondom == 1 ~ 3,
            .male & mot == 0 & yr_last_f >= p10y ~ 3,
            .male & mot == 0 & year(expose_sex_f_av_date) >= p10y ~ 3,          # HTS Form
            .male & mot == 0 & year(expose_sex_f_av_nocondom_date) >= p10y ~ 3, # HTS Form
            TRUE ~ mot
         ),

         # f->m
         mot        = case_when(
            .female & expose_sex_m_nocondom == 1 ~ 4,
            .female & yr_last_m >= p10y ~ 4,
            .female & year(expose_sex_m_av_date) >= p10y ~ 4,          # HTS Form
            .female & year(expose_sex_m_av_nocondom_date) >= p10y ~ 4, # HTS Form
            TRUE ~ mot
         ),

         # IVDU
         mot        = case_when(
            expose_drug_inject > 0 & str_left(perm_prov, 5) %in% cebu ~ 5,
            TRUE ~ mot
         ),

         # vertical
         mot        = case_when(
            mot == 0 & motherisi1 == 1 ~ 6,
            TRUE ~ mot
         ),

         # m->m-f hx
         mot        = case_when(
            .male & mot == 0 & num_m_partner > 0 & is.na(yr_last_m) ~ 11,
            .male & mot == 0 & yr_last_m >= p10y ~ 11,
            .male & mot == 0 & !is.na(expose_sex_m_av_date) ~ 11,          # HTS Form
            .male & mot == 0 & !is.na(expose_sex_m_av_nocondom_date) ~ 11, # HTS Form
            .male & mot == 0 & expose_sex_m > 0 ~ 11,                      # HTS Form
            TRUE ~ mot
         ),

         # m->m+f hx
         mot        = case_when(
            mot == 1 & num_f_partner > 0 & is.na(yr_last_f) ~ 21,
            mot == 3 & num_m_partner > 0 & is.na(yr_last_m) ~ 21,
            mot == 11 & num_f_partner > 0 & is.na(yr_last_f) ~ 21,
            mot == 11 & yr_last_f >= p10y ~ 21,
            mot == 11 & !is.na(expose_sex_f_av_date) ~ 21,          # HTS Form
            mot == 11 & !is.na(expose_sex_f_av_nocondom_date) ~ 21, # HTS Form
            mot == 11 & expose_sex_f > 0 ~ 21,                      # HTS Form
            TRUE ~ mot
         ),

         # m->f hx
         mot        = case_when(
            .male & mot == 0 & num_f_partner > 0 & is.na(yr_last_f) ~ 31,
            .male & mot == 0 & yr_last_f >= p10y ~ 31,
            .male & mot == 0 & !is.na(expose_sex_f_av_date) ~ 31,          # HTS Form
            .male & mot == 0 & !is.na(expose_sex_f_av_nocondom_date) ~ 31, # HTS Form
            .male & mot == 0 & expose_sex_f > 0 ~ 31,                      # HTS Form
            TRUE ~ mot
         ),

         # f->m hx
         mot        = case_when(
            .female & mot == 0 & num_m_partner > 0 & is.na(yr_last_m) ~ 41,
            .female & mot == 0 & yr_last_m >= p10y ~ 41,
            .female & mot == 0 & !is.na(expose_sex_m_av_date) ~ 41,          # HTS Form
            .female & mot == 0 & !is.na(expose_sex_m_av_nocondom_date) ~ 41, # HTS Form
            .female & mot == 0 & expose_sex_m > 0 ~ 41,                      # HTS Form
            TRUE ~ mot
         ),

         # IVDU hx
         mot        = case_when(
            injectdrug > 0 & str_left(perm_prov, 5) %in% cebu ~ 51,
            TRUE ~ mot
         ),

         # mtct
         mot        = case_when(
            mot == 0 & age < 5 ~ 61,
            TRUE ~ mot
         ),

         # all else fails
         mot        = case_when(
            .male & mot == 0 & num_m_partner > 0 ~ 1,
            TRUE ~ mot
         ),
         mot        = case_when(
            mot == 1 & num_f_partner > 0 ~ 2,
            TRUE ~ mot
         ),

         # needlestick
         mot        = case_when(
            mot == 0 & needlepri1 == 1 ~ 7,
            TRUE ~ mot
         ),

         # transfusion
         mot        = case_when(
            mot == 0 & receivedbt == 1 ~ 8,
            TRUE ~ mot
         ),

         # f->f, must run before "no data" since it needs mot == 0
         mot        = case_when(
            .female & mot == 0 & num_f_partner > 0 ~ 10,
            .female & mot == 0 & !is.na(yr_last_f) ~ 10,
            .female & mot == 0 & !is.na(expose_sex_f_av_date) ~ 10,
            .female & mot == 0 & !is.na(expose_sex_f_av_nocondom_date) ~ 10,
            .female & mot == 0 & expose_sex_f > 0 ~ 10,
            TRUE ~ mot
         ),

         # no data
         mot        = case_when(
            mot == 0 ~ 9,
            TRUE ~ mot
         ),

         # final filtering of mot using risk_*
         mot        = case_when(
            mot %in% c(7, 8, 9, 10) &
               .male &
               str_detect(risk_sexwithm, "^yes") &
               str_detect(risk_sexwithf, "^yes") ~ 22,
            mot %in% c(7, 8, 9, 10) &
               .male &
               str_detect(risk_sexwithm, "^yes") &
               !str_detect(risk_sexwithf, "^yes") ~ 12,
            mot %in% c(7, 8, 9, 10) &
               .male &
               !str_detect(risk_sexwithm, "^yes") &
               str_detect(risk_sexwithf, "^yes") ~ 32,
            mot %in% c(7, 8, 9, 10) &
               .female &
               str_detect(risk_sexwithm, "^yes") &
               !str_detect(risk_sexwithf, "^yes") ~ 42,
            mot %in% c(7, 8, 9, 10) & str_detect(risk_injectdrug, "^yes") ~ 52,
            TRUE ~ mot
         ),

         # transmit
         transmit   = case_when(
            mot %in% c(1, 2, 3, 4, 11, 12, 21, 22, 31, 32, 41, 42) ~ "SEX",
            mot %in% c(5, 51, 52) ~ "IVDU",
            mot %in% c(6, 61) ~ "PERINATAL",
            mot %in% c(8, 9, 10) ~ "UNKNOWN",
            mot == 7 ~ "OTHERS",
         ),

         # sexhow
         sexhow     = case_when(
            mot %in% c(1, 11, 12) ~ "HOMOSEXUAL",
            mot %in% c(2, 21, 22) ~ "BISEXUAL",
            mot %in% c(3, 4, 31, 32, 41, 42) ~ "HETEROSEXUAL",
         ),
      ) %>%
      select(-.male, -.female)

   return(data)
}

# process hts data
process_hts <- function(form_hts, form_a, form_cfbs, testing = data.frame()) {
   log_info("Combining forms.")
   # use hts form as base
   hts <- form_hts %>%
      as_tibble() %>%
      mutate(
         form_version = "HTS Form (v2021)",
      ) %>%
      bind_rows(
         # second priority - form a
         form_a %>%
            as_tibble() %>%
            mutate(
               form_version = case_when(
                  form_id == 'a2011' ~ "Form A (v2011)",
                  form_id == 'a2014' ~ "Form A (v2014)",
                  form_id == 'a2017' ~ "Form A (v2017)",
                  TRUE ~ "Form A (v2017)"
               ),
            ),
         # lastly - cfbs form
         form_cfbs %>%
            as_tibble() %>%
            mutate(
               form_version = "CFBS Form (v2020)",
            ) %>%
            rename(
               t0_date         = test_date,
               t0_result       = test_result,
               service_condoms = service_given_condoms,
               service_lubes   = service_given_lubes,
            ) %>%
            rename_with(
               ~stri_replace_first_fixed(., "risk_", "expose_"),
               starts_with("risk_")
            )
      ) %>%
      distinct(rec_id, .keep_all = TRUE)

   if (nrow(testing) > 0) {
      test_same <- intersect(names(testing), names(hts))
      test_diff <- c('rec_id', setdiff(names(testing), names(hts)))

      # t0 comes from testing when it has any, otherwise keep the form's own t0
      t0_cols <- intersect(grep("^t0_", names(testing), value = TRUE), names(hts))
      test_t0 <- testing %>%
         select(rec_id, all_of(t0_cols)) %>%
         filter(if_any(all_of(t0_cols), ~!is.na(.))) %>%
         distinct(rec_id, .keep_all = TRUE)
      t0_row  <- match(hts$rec_id, test_t0$rec_id)
      for (col in t0_cols) {
         hts[[col]] <- if_else(!is.na(t0_row), test_t0[[col]][t0_row], hts[[col]])
      }

      hts %<>%
         bind_rows(testing %>% select(any_of(test_same))) %>%
         left_join(
            y  = testing %>%
               select(any_of(test_diff)),
            by = join_by(rec_id)
         )
   }

   hts %<>%
      distinct(rec_id, .keep_all = TRUE) %>%
      # make simplified tagging for source form
      mutate(
         src = form_version,
         src = stri_replace_first_fixed(src, "Form", ""),
         src = stri_replace_first_fixed(src, " (v", ""),
         src = stri_replace_first_fixed(src, ")", ""),
         src = tolower(stri_replace_all_fixed(src, " ", "")),

         # source form flags
         is_form_a = src %in% c("a2011", "a2014", "a2017"),
         is_hts    = src == "hts2021",
         is_cfbs   = src == "cfbs2020",
      ) %>%
      # test information
      mutate(
         across(
            c(
               t0_result,
               t1_result,
               t2_result,
               t3_result,
               confirm_result,
               modality,
               screen_agreed
            ),
            ~keep_code(.)
         )
      ) %>%
      # results
      mutate(
         hts_date        = case_when(
            t0_date >= -25567 & interval(record_date, t0_date) / years(1) <= -2 ~ as.Date(record_date),
            t0_date >= -25567 & interval(record_date, t0_date) / years(1) > -2 ~ as.Date(t0_date),
            !is.na(date_collect) ~ as.Date(date_collect),
            t1_date < record_date ~ as.Date(t1_date),
            TRUE ~ as.Date(record_date)
         ),
         hts_result      = case_when(
            confirm_result == 1 ~ "R",
            confirm_result == 2 ~ "NR",
            confirm_result == 3 ~ "IND",
            t3_result == 1 ~ "R",
            t3_result == 2 ~ "NR",
            t3_result == 3 ~ "IND",
            t2_result == 1 ~ "R",
            t2_result == 2 ~ "NR",
            t1_result == 1 ~ "R",
            t1_result == 2 ~ "NR",
            t0_result == 1 ~ "R",
            t0_result == 2 ~ "NR",
            grepl("HIV-NR", toupper(clinic_notes)) ~ "NR",
            grepl("HIN NR", toupper(clinic_notes)) ~ "NR",
            grepl("HIV-NR", toupper(counsel_notes)) ~ "NR",
            TRUE ~ "(no data)"
         ),
         hts_modality    = case_when(
            screen_agreed == 0 ~ "REACH",
            is.na(screen_agreed) & hts_result == "(no data)" ~ "REACH",
            confirm_result != 4 & is.na(modality) ~ "FBT",
            is_form_a ~ "FBT",
            is_cfbs & hts_result != "(no data)" ~ "CBS",
            is_hts &
               is.na(modality) &
               hts_result != "(no data)" ~ "FBT",
            faci_id == "130605" ~ "CBS",
            modality == "101101" ~ "FBT",
            modality == "101103" ~ "CBS",
            modality == "101104" ~ "FBS",
            modality == "101105" ~ "ST",
            modality == "101304" ~ "REACH",
            TRUE ~ "(no data)"
         ),
         test_agreed     = case_when(
            screen_agreed == 0 ~ 0,
            screen_agreed == 1 ~ 1,
            !(hts_modality %in% c("REACH", "(no data)")) ~ 1,
            hts_result != "(no data)" ~ 1,
            TRUE ~ 0
         ),
         client_type_1   = str_left(client_type, 1),
         hts_client_type = case_when(
            client_type_1 == "1" ~ "Inpatient",
            hts_modality == "ST" ~ "ST",
            hts_modality %in% c("CBS", "FBS") ~ "CBS",
            client_type_1 == "3" ~ "CBS",
            client_type_1 == "7" ~ "PDL",
            TRUE ~ "Walk-in"
         )
      )

   log_info("Tagging risks.")
   data <- hts %>%
      # risk information
      mutate(
         across(
            starts_with("expose_", ignore.case = FALSE) & !contains("date"),
            ~as.integer(keep_code(.))
         )
      ) %>%
      mutate(
         risk_motherhashiv     = case_when(
            expose_hiv_mother %in% c(1, 2) ~ "yes",
            expose_hiv_mother == 0 ~ "no",
            TRUE ~ "(no data)"
         ),

         # sex with female
         recent_sexwithf       = months_since(expose_sex_f_av_date, record_date),
         recent_sexwithf       = case_when(
            recent_sexwithf <= 1 ~ "p01m",
            recent_sexwithf <= 3 ~ "p03m",
            recent_sexwithf <= 6 ~ "p06m",
            recent_sexwithf <= 12 ~ "p12m",
            yr_last_f == year(hts_date) ~ "p12m",
            is_cfbs &
               (recent_sexwithf > 12 | is.na(recent_sexwithf)) &
               num_f_partner > 0 ~ "p12m",
            !is_cfbs &
               (recent_sexwithf > 12 | is.na(recent_sexwithf)) &
               num_f_partner > 0 ~ "beyond_p12m",
            recent_sexwithf > 12 ~ "beyond_p12m",
            yr_last_f != year(hts_date) ~ "beyond_p12m",
            num_f_partner == 0 ~ "none",
            TRUE ~ "(no data)"
         ),
         recent_sexwithf_nocdm = bucket_recency(case_when(
            is_hts ~ months_since(expose_sex_f_av_nocondom_date, record_date),
            is_cfbs ~ months_since(expose_condomless_vaginal_date, record_date),
         )),
         # consolidate both categories
         recent_sexwithf_c     = combine_recency(recent_sexwithf, recent_sexwithf_nocdm),

         risk_sexwithf         = case_when(
            # form a
            is_form_a & expose_sex_f_nocondom == 0 ~ "none",
            is_form_a & expose_sex_f_nocondom == 1 ~ "yes-p12m",
            is_form_a & expose_sex_f_nocondom == 2 ~ "yes-beyond_p12m",
            is_form_a & is.na(expose_sex_f_nocondom) ~ "(no data)",

            # hts form
            is_hts &
               expose_sex_f == 0 &
               (expose_sex_f_av_nocondom == 0 | is.na(expose_sex_f_av_nocondom)) &
               recent_sexwithf_c %in% c("none", "(no data)") ~ "none",
            is_hts &
               expose_sex_f_av_nocondom == 0 &
               (expose_sex_f == 0 | is.na(expose_sex_f)) &
               recent_sexwithf_c %in% c("none", "(no data)") ~ "none",
            is_hts &
               (expose_sex_f == 1 | expose_sex_f_av_nocondom == 1) &
               recent_sexwithf_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_hts &
               (expose_sex_f == 1 | expose_sex_f_av_nocondom == 1) &
               !(recent_sexwithf_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_c),
            is_hts &
               (expose_sex_f == 0 | expose_sex_f_av_nocondom == 0) &
               !(recent_sexwithf_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_c),
            is_hts &
               is.na(expose_sex_f) &
               is.na(expose_sex_f_av_nocondom) &
               !(recent_sexwithf_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_c),
            is_hts &
               is.na(expose_sex_f) &
               is.na(expose_sex_f_av_nocondom) &
               recent_sexwithf_c %in% c("none", "(no data)") ~ recent_sexwithf_c,

            # cfbs form
            is_cfbs &
               expose_condomless_vaginal == 0 &
               recent_sexwithf_c %in% c("none", "(no data)") ~ "none",
            is_cfbs & !(recent_sexwithf_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_c),
            is_cfbs & expose_condomless_vaginal == 2 ~ "yes-beyond_p12m",
            is_cfbs &
               is.na(expose_condomless_vaginal) &
               recent_sexwithf_c %in% c("none", "(no data)") ~ recent_sexwithf_c,
         ),
         risk_sexwithf_nocdm   = case_when(
            # form a
            is_form_a & expose_sex_f_nocondom == 0 ~ "none",
            is_form_a & expose_sex_f_nocondom == 1 ~ "yes-p12m",
            is_form_a & expose_sex_f_nocondom == 2 ~ "yes-beyond_p12m",
            is_form_a & is.na(expose_sex_f_nocondom) ~ "(no data)",

            # hts form
            is_hts &
               expose_sex_f_av_nocondom == 0 &
               recent_sexwithf_nocdm %in% c("none", "(no data)") ~ "none",
            is_hts &
               expose_sex_f_av_nocondom == 1 &
               recent_sexwithf_nocdm %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_hts &
               expose_sex_f_av_nocondom == 1 &
               !(recent_sexwithf_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_nocdm),
            is_hts &
               expose_sex_f_av_nocondom == 0 &
               !(recent_sexwithf_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_nocdm),
            is_hts &
               is.na(expose_sex_f_av_nocondom) &
               !(recent_sexwithf_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_nocdm),
            is_hts &
               is.na(expose_sex_f_av_nocondom) &
               recent_sexwithf_nocdm %in% c("none", "(no data)") ~ recent_sexwithf_nocdm,

            # cfbs form
            is_cfbs &
               expose_condomless_vaginal == 0 &
               recent_sexwithf_nocdm %in% c("none", "(no data)") ~ "none",
            is_cfbs & !(recent_sexwithf_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_nocdm),
            is_cfbs & expose_condomless_vaginal == 2 ~ "yes-beyond_p12m",
            is_cfbs &
               is.na(expose_condomless_vaginal) &
               recent_sexwithf_nocdm %in% c("none", "(no data)") ~ recent_sexwithf_nocdm,
         ),


         # sex with male
         recent_sexwithm       = months_since(expose_sex_m_av_date, record_date),
         recent_sexwithm       = case_when(
            recent_sexwithm <= 1 ~ "p01m",
            recent_sexwithm <= 3 ~ "p03m",
            recent_sexwithm <= 6 ~ "p06m",
            recent_sexwithm <= 12 ~ "p12m",
            yr_last_m == year(hts_date) ~ "p12m",
            is_cfbs &
               (recent_sexwithm > 12 | is.na(recent_sexwithm)) &
               num_m_partner > 0 ~ "p12m",
            !is_cfbs &
               (recent_sexwithm > 12 | is.na(recent_sexwithm)) &
               num_m_partner > 0 ~ "beyond_p12m",
            recent_sexwithm > 12 ~ "beyond_p12m",
            yr_last_m != year(hts_date) ~ "beyond_p12m",
            num_m_partner == 0 ~ "none",
            TRUE ~ "(no data)"
         ),
         recent_sexwithm_nocdm = bucket_recency(case_when(
            is_hts ~ months_since(expose_sex_m_av_nocondom_date, record_date),
            is_cfbs ~ months_since(expose_condomless_anal_date, record_date),
         )),
         # consolidate both categories
         recent_sexwithm_c     = combine_recency(recent_sexwithm, recent_sexwithm_nocdm),

         risk_sexwithm         = case_when(
            # form a
            is_form_a & expose_sex_m_nocondom == 0 ~ "none",
            is_form_a & expose_sex_m_nocondom == 1 ~ "yes-p12m",
            is_form_a & expose_sex_m_nocondom == 2 ~ "yes-beyond_p12m",
            is_form_a & is.na(expose_sex_m_nocondom) ~ "(no data)",

            # hts form
            is_hts &
               expose_sex_m == 0 &
               (expose_sex_m_av_nocondom == 0 | is.na(expose_sex_m_av_nocondom)) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "none",
            is_hts &
               expose_sex_m_av_nocondom == 0 &
               (expose_sex_m == 0 | is.na(expose_sex_m)) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "none",
            is_hts &
               (expose_sex_m == 1 | expose_sex_m_av_nocondom == 1) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_hts &
               (expose_sex_m == 1 | expose_sex_m_av_nocondom == 1) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            is_hts &
               (expose_sex_m == 0 | expose_sex_m_av_nocondom == 0) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            is_hts &
               is.na(expose_sex_m) &
               is.na(expose_sex_m_av_nocondom) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            is_hts &
               is.na(expose_sex_m) &
               is.na(expose_sex_m_av_nocondom) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ recent_sexwithm_c,

            # cfbs form
            is_cfbs &
               expose_condomless_anal == 0 &
               (expose_m_sex_oral_anal == 0 | is.na(expose_m_sex_oral_anal)) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "none",
            is_cfbs &
               expose_m_sex_oral_anal == 0 &
               (expose_condomless_anal == 0 | is.na(expose_condomless_anal)) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "none",
            is_cfbs &
               (expose_condomless_anal == 1 | expose_m_sex_oral_anal == 1) &
               recent_sexwithm_c %in% c("none", "(no data)", "beyond_p12m") ~ "yes-p12m",
            is_cfbs &
               (expose_condomless_anal == 2 | expose_m_sex_oral_anal == 2) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_cfbs &
               (expose_condomless_anal == 2 | expose_m_sex_oral_anal == 2) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            is_cfbs &
               (expose_condomless_anal == 0 | expose_m_sex_oral_anal == 0) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            is_cfbs &
               is.na(expose_condomless_anal) &
               is.na(expose_m_sex_oral_anal) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            is_cfbs &
               is.na(expose_condomless_anal) &
               is.na(expose_m_sex_oral_anal) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ recent_sexwithm_c,
         ),
         risk_sexwithm_nocdm   = case_when(
            # form a
            is_form_a & expose_sex_m_nocondom == 0 ~ "none",
            is_form_a & expose_sex_m_nocondom == 1 ~ "yes-p12m",
            is_form_a & expose_sex_m_nocondom == 2 ~ "yes-beyond_p12m",
            is_form_a & is.na(expose_sex_m_nocondom) ~ "(no data)",

            # hts form
            is_hts &
               expose_sex_m_av_nocondom == 0 &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ "none",
            is_hts &
               expose_sex_m_av_nocondom == 1 &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_hts &
               expose_sex_m_av_nocondom == 1 &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            is_hts &
               expose_sex_m_av_nocondom == 0 &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            is_hts &
               is.na(expose_sex_m_av_nocondom) &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            is_hts &
               is.na(expose_sex_m_av_nocondom) &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ recent_sexwithm_nocdm,

            # cfbs form
            is_cfbs &
               expose_condomless_anal == 0 &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ "none",
            is_cfbs &
               expose_condomless_anal == 1 &
               recent_sexwithm_nocdm %in% c("none", "(no data)", "beyond_p12m") ~ "yes-p12m",
            is_cfbs &
               expose_condomless_anal == 2 &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_cfbs &
               expose_condomless_anal == 2 &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            is_cfbs &
               expose_condomless_anal == 0 &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            is_cfbs &
               is.na(expose_condomless_anal) &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            is_cfbs &
               is.na(expose_condomless_anal) &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ recent_sexwithm_nocdm,
         ),

         # paid for sex / sex worker
         recent_payingforsex   = bucket_recency(months_since(expose_sex_paying_date, record_date)),
         risk_payingforsex     = case_when(
            is_form_a & expose_sex_paying == 1 ~ "yes-p12m",
            is_form_a & expose_sex_paying == 2 ~ "yes-beyond_p12m",
            is_form_a & expose_sex_paying == 0 ~ "none",
            is_form_a & is.na(expose_sex_paying) ~ "(no data)",
            is_cfbs ~ "(no data)",
            is_hts &
               expose_sex_paying == 1 &
               !(recent_payingforsex %in% c("none", "(no data)")) ~ paste0("yes-", recent_payingforsex),
            is_hts &
               expose_sex_paying == 1 &
               recent_payingforsex %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_hts &
               expose_sex_paying == 0 &
               recent_payingforsex %in% c("none", "(no data)") ~ "none",
            is_hts & is.na(expose_sex_paying) ~ "(no data)"
         ),

         # paid for sex / sex worker
         recent_paymentforsex  = bucket_recency(months_since(expose_sex_payment_date, record_date)),
         risk_paymentforsex    = case_when(
            is_form_a & expose_sex_payment == 1 ~ "yes-p12m",
            is_form_a & expose_sex_payment == 2 ~ "yes-beyond_p12m",
            is_form_a & expose_sex_payment == 0 ~ "none",
            is_form_a & is.na(expose_sex_payment) ~ "(no data)",
            is_hts &
               expose_sex_payment == 1 &
               !(recent_paymentforsex %in% c("none", "(no data)")) ~ paste0("yes-", recent_paymentforsex),
            is_hts &
               expose_sex_payment == 1 &
               recent_paymentforsex %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_hts &
               expose_sex_payment == 0 &
               recent_paymentforsex %in% c("none", "(no data)") ~ "none",
            is_hts & is.na(expose_sex_payment) ~ "(no data)",
            is_cfbs &
               expose_sex_payment == 2 &
               !(recent_paymentforsex %in% c("none", "(no data)")) ~ paste0("yes-", recent_paymentforsex),
            is_cfbs &
               expose_sex_payment == 2 &
               recent_paymentforsex %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_cfbs &
               expose_sex_payment == 1 &
               !(recent_paymentforsex %in% c("none", "(no data)", "beyond_p12m")) ~ paste0("yes-", recent_paymentforsex),
            is_cfbs &
               expose_sex_payment == 1 &
               recent_paymentforsex %in% c("none", "(no data)", "beyond_p12m") ~ "yes-p12m",
            is_cfbs &
               expose_sex_payment == 0 &
               recent_paymentforsex %in% c("none", "(no data)") ~ "none",
            is_cfbs &
               is.na(expose_sex_payment) &
               recent_paymentforsex == "(no data)" ~ "(no data)",
         ),

         # sex w/ someone who has HIV
         recent_sexwithhiv     = bucket_recency(months_since(expose_sex_hiv_date, record_date)),
         risk_sexwithhiv       = case_when(
            is_form_a & expose_sex_hiv == 1 ~ "yes-p12m",
            is_form_a & expose_sex_hiv == 2 ~ "yes-beyond_p12m",
            is_form_a & expose_sex_hiv == 0 ~ "none",
            is_form_a & is.na(expose_sex_hiv) ~ "(no data)",
            is_hts ~ "(no data)",
            is_cfbs &
               expose_sex_hiv == 2 &
               !(recent_sexwithhiv %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithhiv),
            is_cfbs &
               expose_sex_hiv == 2 &
               recent_sexwithhiv %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_cfbs &
               expose_sex_hiv == 0 &
               recent_sexwithhiv %in% c("none", "(no data)") ~ "none",
            is_cfbs & is.na(expose_sex_hiv) ~ "(no data)"
         ),

         # shared injects / injecting drugs
         recent_injectdrug     = bucket_recency(months_since(expose_drug_inject_date, record_date)),
         recent_injectshare    = bucket_recency(months_since(expose_needle_share_date, record_date)),
         # consolidate both categories
         recent_injectdrug_c   = combine_recency(recent_injectshare, recent_injectdrug),

         risk_injectdrug       = case_when(
            # form a
            is_form_a & expose_drug_inject == 1 ~ "yes-p12m",
            is_form_a & expose_drug_inject == 2 ~ "yes-beyond_p12m",
            is_form_a & expose_drug_inject == 0 ~ "none",
            is_form_a & is.na(expose_drug_inject) ~ "(no data)",

            # hts form
            is_hts &
               expose_drug_inject == 1 &
               !(recent_injectdrug_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_injectdrug_c),
            is_hts &
               expose_drug_inject == 1 &
               recent_injectdrug_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_hts &
               expose_drug_inject == 0 &
               recent_injectdrug_c %in% c("none", "(no data)") ~ "none",
            is_hts & is.na(expose_drug_inject) ~ "(no data)",

            # cfbs form
            is_cfbs &
               expose_drug_inject == 2 &
               (expose_needle_share %in% c(0, 2) | is.na(expose_needle_share)) &
               !(recent_injectdrug_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_injectdrug_c),
            is_cfbs &
               expose_needle_share == 2 &
               (expose_drug_inject %in% c(0, 2) | is.na(expose_drug_inject)) &
               !(recent_injectdrug_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_injectdrug_c),
            is_cfbs &
               expose_drug_inject == 2 &
               (expose_needle_share %in% c(0, 2) | is.na(expose_needle_share)) &
               recent_injectdrug_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_cfbs &
               expose_needle_share == 2 &
               (expose_drug_inject %in% c(0, 2) | is.na(expose_drug_inject)) &
               recent_injectdrug_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_cfbs &
               expose_drug_inject == 1 &
               !(recent_injectdrug_c %in% c("none", "(no data)", "beyond_p12m")) ~ paste0("yes-", recent_injectdrug_c),
            is_cfbs &
               expose_drug_inject == 1 &
               recent_injectdrug_c %in% c("none", "(no data)", "beyond_p12m") ~ "yes-p12m",
            is_cfbs &
               expose_drug_inject == 0 &
               !(recent_injectdrug_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_injectdrug_c),
            is_cfbs &
               expose_drug_inject == 0 &
               recent_injectdrug_c %in% c("none", "(no data)") ~ "none",
            is_cfbs &
               is.na(expose_drug_inject) &
               recent_injectdrug_c == "(no data)" ~ "(no data)",
            is_cfbs &
               is.na(expose_drug_inject) &
               !(recent_injectdrug_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_injectdrug_c),
         ),

         # occupational exposure
         recent_needlestick    = bucket_recency(months_since(expose_occupation_date, record_date)),
         risk_needlestick      = case_when(
            is_form_a & expose_occupation == 1 ~ "yes-p12m",
            is_form_a & expose_occupation == 2 ~ "yes-beyond_p12m",
            is_form_a & expose_occupation == 0 ~ "none",
            is_form_a & is.na(expose_occupation) ~ "(no data)",
            is_cfbs ~ "(no data)",
            is_hts &
               expose_occupation == 1 &
               !(recent_needlestick %in% c("none", "(no data)")) ~ paste0("yes-", recent_needlestick),
            is_hts &
               expose_occupation == 1 &
               recent_needlestick %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_hts &
               expose_occupation == 0 &
               recent_needlestick %in% c("none", "(no data)") ~ "none",
            is_hts & is.na(expose_occupation) ~ "(no data)"
         ),

         # blood transfusion
         recent_bloodtransfuse = bucket_recency(months_since(expose_blood_transfuse_date, record_date)),
         risk_bloodtransfuse   = case_when(
            is_form_a & expose_blood_transfuse == 1 ~ "yes-p12m",
            is_form_a & expose_blood_transfuse == 2 ~ "yes-beyond_p12m",
            is_form_a & expose_blood_transfuse == 0 ~ "none",
            is_form_a & is.na(expose_blood_transfuse) ~ "(no data)",
            is_cfbs ~ "(no data)",
            is_hts &
               expose_blood_transfuse == 1 &
               !(recent_bloodtransfuse %in% c("none", "(no data)")) ~ paste0("yes-", recent_bloodtransfuse),
            is_hts &
               expose_blood_transfuse == 1 &
               recent_bloodtransfuse %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_hts &
               expose_blood_transfuse == 0 &
               recent_bloodtransfuse %in% c("none", "(no data)") ~ "none",
            is_hts & is.na(expose_blood_transfuse) ~ "(no data)"
         ),

         # chemsex & drugs
         recent_illicitdrug    = bucket_recency(months_since(expose_illicit_drugs_date, record_date)),
         risk_illicitdrug      = case_when(
            is_form_a ~ "(no data)",
            is_hts ~ "(no data)",
            is_cfbs &
               expose_illicit_drugs == 2 &
               !(recent_illicitdrug %in% c("none", "(no data)")) ~ paste0("yes-", recent_illicitdrug),
            is_cfbs &
               expose_illicit_drugs == 2 &
               recent_illicitdrug %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_cfbs &
               expose_illicit_drugs == 0 &
               recent_illicitdrug %in% c("none", "(no data)") ~ "none",
            is_cfbs & is.na(expose_illicit_drugs) ~ "(no data)"
         ),

         # had sex under influence of drugs
         recent_chemsex        = bucket_recency(months_since(expose_sex_drugs_date, record_date)),
         risk_chemsex          = case_when(
            is_form_a ~ "(no data)",
            is_cfbs ~ "(no data)",
            is_hts &
               expose_sex_drugs == 1 &
               !(recent_chemsex %in% c("none", "(no data)")) ~ paste0("yes-", recent_chemsex),
            is_hts &
               expose_sex_drugs == 1 &
               recent_chemsex %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            is_hts &
               expose_sex_drugs == 0 &
               recent_chemsex %in% c("none", "(no data)") ~ "none",
            is_hts & is.na(expose_sex_drugs) ~ "(no data)"
         ),

         # tattoo
         risk_tattoo           = case_when(
            is_form_a & expose_tattoo == 1 ~ "yes-p12m",
            is_form_a & expose_tattoo == 2 ~ "yes-beyond_p12m",
            is_form_a & expose_tattoo == 0 ~ "none",
            is_form_a & is.na(expose_tattoo) ~ "(no data)",
            is_cfbs ~ "(no data)",
            is_hts ~ "(no data)",
         ),

         # sti
         risk_sti              = case_when(
            is_form_a & expose_sti == 1 ~ "yes-p12m",
            is_form_a & expose_sti == 2 ~ "yes-beyond_p12m",
            is_form_a & expose_sti == 0 ~ "none",
            is_form_a & is.na(expose_sti) ~ "(no data)",
            is_cfbs ~ "(no data)",
            is_hts ~ "(no data)",
         ),

         p10y                  = year(hts_date %m-% years(10)),
      ) %>%
      generate_mot(sex == "1_Male", sex == "2_Female") %>%
      mutate(
         mot = labelled(
            mot,
            c(
               'M->M only'               = 1,
               'M->(M+F) sex'            = 2,
               'M->F only'               = 3,
               'F->M'                    = 4,
               'IVDU (Cebu province)'    = 5,
               'Vertical'                = 6,
               'Needlestick'             = 7,
               'Transfusion'             = 8,
               'M->M hx'                 = 11,
               'M->(M+F) hx'             = 21,
               'M->F hx'                 = 31,
               'F->M hx'                 = 41,
               'IVDU hx (Cebu province)' = 51,
               'Vertical (<5 y.o.)'      = 61,
               'No risk'                 = 9,
               'F->F only'               = 10,
               'M->(M+F) unreliable'     = 22,
               'M->M unreliable'         = 12,
               'M->F unreliable'         = 32,
               'F->M unreliable'         = 42,
               'IVDU unreliable'         = 52
            )
         )
      ) %>%
      select(-starts_with("recent_", ignore.case = FALSE)) %>%
      mutate(
         # process reach types
         cbs_venue      = toupper(str_squish(hiv_service_addr)),
         online_app     = case_when(
            grepl("GRINDR|GRNDR|GRINDER", cbs_venue) ~ "GRINDR",
            grepl("TWITTER", cbs_venue) ~ "TWITTER",
            grepl("FACEBOOK|MESSENGER|\\bFB\\b", cbs_venue) ~ "FACEBOOK",
            grepl("\\bGR\\b", cbs_venue) ~ "GRINDR",
         ),
         reach_online   = if_else(!is.na(online_app), "1_Yes", reach_online, reach_online),
         reach_clinical = if_else(
            condition = if_all(starts_with("reach_"), ~is.na(.)) & hts_modality == "FBT",
            true      = "1_Yes",
            false     = reach_clinical,
            missing   = reach_clinical
         )
      ) %>%
      select(
         -any_of(
            c(
               "prime",
               "disease",
               "hiv_service_type",
               "src",
               "module",
               "modality",
               "confirmatory_code",
               "children..50"
            )
         )
      ) %>%
      # restore the uncoded expose_* values (row order is unchanged since hts)
      mutate(!!!as.list(select(hts, starts_with("expose_")))) %>%
      relocate(any_of(names(hts)), .before = 1)

   log_info("Combining risks.")
   # distinct risk values per record, sorted & comma-separated
   risk_vals <- select(data, starts_with("risk_", ignore.case = FALSE))
   risks     <- rep("", nrow(data))
   for (lvl in sort(unique(unlist(risk_vals, use.names = FALSE)))) {
      has_lvl <- Reduce(`|`, lapply(risk_vals, function(col) coalesce(col == lvl, FALSE)))
      risks   <- if_else(has_lvl, if_else(risks == "", lvl, stri_c(risks, ", ", lvl)), risks)
   }

   log_info("Finalizing KPs.")
   data %<>%
      mutate(
         risks       = risks,
         sexual_risk = case_when(
            str_detect(risk_sexwithm, "yes") & str_detect(risk_sexwithf, "yes") ~ "M+F",
            str_detect(risk_sexwithm, "yes") & !str_detect(risk_sexwithf, "yes") ~ "M",
            !str_detect(risk_sexwithm, "yes") & str_detect(risk_sexwithf, "yes") ~ "F",
         ),
         kap_unknown = if_else(coalesce(risks, "(no data)") == "(no data)", "(no data)", NA_character_),
         kap_msm     = if_else(sex == "1_Male" & sexual_risk %in% c("M", "M+F"), "MSM", NA_character_),
         kap_heterom = if_else(sex == "1_Male" & sexual_risk == "F", "Hetero Male", NA_character_),
         kap_heterof = if_else(sex == "2_Female" & !is.na(sexual_risk), "Hetero Female", NA_character_),
         kap_pwid    = if_else(str_detect(risk_injectdrug, "yes"), "PWID", NA_character_),
         kap_pip     = if_else(str_detect(risk_paymentforsex, "yes"), "PIP", NA_character_),
         kap_pdl     = if_else(client_type_1 == "7", "PDL", NA_character_),
      ) %>%
      select(-is_form_a, -is_hts, -is_cfbs, -client_type_1)

   return(data)
}

convert_hts <- function(hts_data, convert_type = c("nhsss", "name", "code")) {
   data <- hts_data %>%
      mutate(
         use_record_faci = if_else(is.na(service_faci), 1, 0, 0),
         service_faci    = if_else(use_record_faci == 1, faci_id, service_faci),

         perm_prov       = if_else(str_left(perm_reg, 2) == "99", "9999900000", perm_prov, perm_prov),
         perm_munc       = if_else(str_left(perm_reg, 2) == "99", "9999999000", perm_munc, perm_munc),
         use_curr        = if_else(
            condition = !is.na(curr_munc) & (is.na(perm_munc) | str_left(perm_munc, 2) == "99"),
            true      = 1,
            false     = 0
         ),
         permcurr_reg    = if_else(
            condition = use_curr == 1,
            true      = curr_reg,
            false     = perm_reg
         ),
         permcurr_prov   = if_else(
            condition = use_curr == 1,
            true      = curr_prov,
            false     = perm_prov
         ),
         permcurr_munc   = if_else(
            condition = use_curr == 1,
            true      = curr_munc,
            false     = perm_munc
         ),


         service_condoms = as.numeric(service_condoms),
         service_lubes   = as.numeric(service_lubes),
      ) %>%
      rename(
         created                 = created_by,
         updated                 = updated_by,
         hts_provider_type       = provider_type,
         hts_provider_type_other = provider_type_other,
      ) %>%
      select(
         -any_of(
            c(
               "prime",
               "disease",
               "hiv_service_type",
               "src",
               "module",
               "modality",
               "confirmatory_code",
               "use_curr"
            )
         )
      ) %>%
      ohasis$get_faci(
         list(report_faci = c("faci_id", "sub_faci_id")),
         convert_type
      ) %>%
      ohasis$get_faci(
         list(hts_faci = c("service_faci", "service_sub_faci")),
         convert_type,
         c("hts_reg", "hts_prov", "hts_munc")
      ) %>%
      ohasis$get_faci(
         list(specimen_source_faci = c("specimen_source", "specimen_sub_source")),
         convert_type
      ) %>%
      ohasis$get_faci(
         list(confirm_lab = c("confirm_faci", "confirm_sub_faci")),
         convert_type
      ) %>%
      get_addr(
         c(
            perm_reg  = "perm_reg",
            perm_prov = "perm_prov",
            perm_munc = "perm_munc"
         ),
         convert_type
      ) %>%
      get_addr(
         c(
            curr_reg  = "curr_reg",
            curr_prov = "curr_prov",
            curr_munc = "curr_munc"
         ),
         convert_type
      ) %>%
      get_addr(
         c(
            permcurr_reg  = "permcurr_reg",
            permcurr_prov = "permcurr_prov",
            permcurr_munc = "permcurr_munc"
         ),
         convert_type
      ) %>%
      get_addr(
         c(
            birth_reg  = "birth_reg",
            birth_prov = "birth_prov",
            birth_munc = "birth_munc"
         ),
         convert_type
      ) %>%
      get_addr(
         c(
            cbs_reg  = "hiv_service_reg",
            cbs_prov = "hiv_service_prov",
            cbs_munc = "hiv_service_munc"
         ),
         convert_type
      ) %>%
      ohasis$get_staff(c(created_by = "created")) %>%
      ohasis$get_staff(c(updated_by = "updated")) %>%
      ohasis$get_staff(c(hts_provider = "provider_id")) %>%
      ohasis$get_staff(c(analyzed_by = "signatory_1")) %>%
      ohasis$get_staff(c(reviewed_by = "signatory_2")) %>%
      ohasis$get_staff(c(noted_by = "signatory_3"))

   return(data)
}

deconstruct_hts <- function(hts) {
   tables <- c(
      "px_pii",
      "px_profile",
      "px_service",
      "px_test",
      "px_ob",
      "px_occupation",
      "px_cfbs",
      "px_ofw",
      "px_expose_hist",
      "px_expose_profile",
      "px_test_reason",
      "px_prev_test",
      "px_med_profile",
      "px_staging",
      "px_reach",
      "px_other_service",
      "px_test_refuse",
      "px_linkage",
      "px_remarks",
      "patients",
      "px_record"
   )

   hts %<>%
      rename_all(tolower) %>%
      mutate_at(
         .vars = vars(
            module,
            sex,
            self_ident,
            civil_status,
            educ_level,
            living_with_partner,
            client_type,
            provider_type,
            t0_result,
            prev_test_result,
            is_pregnant,
            is_student,
            is_employed,
            is_ofw,
            screen_agreed,
            clinical_pic,
            who_class,
            refer_art,
            refer_confirm,
            ofw_station,
            prev_tested,
            signature,
            verbal_consent
         ),
         ~keep_code(.)
      ) %>%
      mutate(
         form_id = 'hts2021'
      ) %>%
      rename(
         location_reg  = hiv_service_reg,
         location_prov = hiv_service_prov,
         location_munc = hiv_service_munc,
         # location_brgy = hiv_service_brgy,
         location_addr = hiv_service_addr,
      ) %>%
      mutate(
         client_mobile = str_replace_all(client_mobile, "[^[:digit:]]", ""),
         client_mobile = case_when(
            str_left(client_mobile, 1) == "9" ~ stri_c("0", client_mobile),
            str_left(client_mobile, 2) == "63" ~ str_replace(client_mobile, "^63", "0"),
            TRUE ~ client_mobile
         ),
         birthdate     = as.character(birthdate)
      )

   # primary keys & columns of all tables, one query each
   log_info("Obtaining {green('Primary Keys')} and {green('Column Names')}.")
   meta <- local({
      conn <- ohasis$conn("db")
      on.exit(dbDisconnect(conn), add = TRUE)

      in_tables <- stri_c("'", tables, "'", collapse = ", ")
      list(
         pks  = dbGetQuery(conn, glue(r"(
            SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = 'ohasis' AND CONSTRAINT_NAME = 'PRIMARY' AND TABLE_NAME IN ({in_tables})
            ORDER BY TABLE_NAME, ORDINAL_POSITION
         )")),
         cols = dbGetQuery(conn, glue(r"(
            SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = 'ohasis' AND TABLE_NAME IN ({in_tables})
            ORDER BY TABLE_NAME, ORDINAL_POSITION
         )"))
      )
   })
   pks  <- lapply(setNames(tables, tables), function(table) meta$pks$column_name[meta$pks$table_name == table])
   cols <- lapply(setNames(tables, tables), function(table) meta$cols$column_name[meta$cols$table_name == table])

   log_info("Creating tables using obtained schema.")
   data        <- lapply(tables, function(table, data, cols) {
      col_need      <- cols[[table]]
      col_not_found <- setdiff(col_need, names(data))

      schema <- data %>%
         mutate(
            !!!setNames(rep(NA_character_, length(col_not_found)), col_not_found)
         ) %>%
         select(any_of(col_need)) %>%
         distinct()

      return(schema)
   }, data = hts, cols = cols)
   names(data) <- tables

   log_info("Manually creating long tables.")
   # long tables
   data$px_expose_hist <- hts %>%
      select(
         any_of(cols$px_expose_hist),
         starts_with("expose_")
      ) %>%
      pivot_longer(
         cols      = starts_with("expose_"),
         names_to  = "exposure",
         values_to = "expose_value"
      ) %>%
      mutate(
         expose_data = if_else(str_detect(exposure, "_date"), "date_last_expose", "is_exposed"),
         exposure    = str_replace(exposure, "^expose_", ""),
         exposure    = str_replace(exposure, "_date$", ""),
         exposure    = case_when(
            exposure == "hiv_mother" ~ "120000",
            exposure == "sex_m" ~ "217000",
            exposure == "sex_m_av" ~ "216000",
            exposure == "sex_m_av_nocondom" ~ "216200",
            exposure == "sex_f" ~ "227000",
            exposure == "sex_f_av" ~ "226000",
            exposure == "sex_f_av_nocondom" ~ "226200",
            exposure == "sex_paying" ~ "200010",
            exposure == "sex_payment" ~ "200020",
            exposure == "sex_drugs" ~ "200300",
            exposure == "drug_inject" ~ "301010",
            exposure == "blood_transfuse" ~ "530000",
            exposure == "occupation" ~ "510000",
            TRUE ~ exposure
         )
      ) %>%
      pivot_wider(
         names_from  = expose_data,
         values_from = expose_value,
      ) %>%
      select(any_of(cols$px_expose_hist)) %>%
      mutate(
         is_exposed = keep_code(is_exposed),
         is_exposed = if_else(!is.na(date_last_expose), "1", is_exposed, is_exposed),
         is_exposed = coalesce(is_exposed, "0"),
      )

   data$px_test <- hts %>%
      select(
         rec_id,
         faci_id,
         sub_faci_id,
         created_by,
         created_at,
         updated_by,
         updated_at,
         starts_with("t0_")
      ) %>%
      filter(!is.na(t0_result) | !is.na(t0_date)) %>%
      rename(
         result       = t0_result,
         date_perform = t0_date
      ) %>%
      mutate(
         test_type = "10",
         test_num  = 1,
         result    = case_when(
            result == "Reactive" ~ "1",
            result == "Non-reactive" ~ "2",
            TRUE ~ result
         ),
      ) %>%
      select(any_of(cols$px_test))

   data$px_test_reason <- hts %>%
      select(
         any_of(cols$px_test_reason),
         starts_with("test_reason")
      ) %>%
      pivot_longer(
         cols      = starts_with("test_reason_"),
         names_to  = "reason",
         values_to = "is_reason"
      ) %>%
      mutate(
         reason_other = if_else(str_detect(reason, "other_text$"), is_reason, NA_character_),
         is_reason    = if_else(!is.na(reason_other), "1_Yes", is_reason, is_reason),
         reason       = str_replace(reason, "^test_reason_", ""),
         reason       = str_replace(reason, "_text$", ""),
         reason       = case_when(
            reason == "hiv_expose" ~ "1",
            reason == "physician" ~ "2",
            reason == "peer_ed" ~ "8",
            reason == "employ_ofw" ~ "3",
            reason == "employ_local" ~ "4",
            reason == "text_email" ~ "9",
            reason == "insurance" ~ "5",
            reason == "other" ~ "8888",
            TRUE ~ reason
         ),
         is_reason    = coalesce(keep_code(is_reason), "0"),
      ) %>%
      filter(is_reason == 1) %>%
      select(any_of(cols$px_test_reason))

   data$px_med_profile <- hts %>%
      select(
         any_of(cols$px_med_profile),
         starts_with("med_")
      ) %>%
      pivot_longer(
         cols      = starts_with("med_"),
         names_to  = "profile",
         values_to = "is_profile"
      ) %>%
      mutate(
         profile    = str_replace(profile, "^med_", ""),
         profile    = case_when(
            profile == "tb_px" ~ "1",
            profile == "sti" ~ "8",
            profile == "hep_b" ~ "3",
            profile == "hep_c" ~ "4",
            profile == "prep_px" ~ "6",
            profile == "pep_px" ~ "7",
            TRUE ~ profile
         ),
         is_profile = coalesce(keep_code(is_profile), "0"),
      ) %>%
      filter(is_profile == 1) %>%
      select(any_of(cols$px_med_profile))

   data$px_reach <- hts %>%
      select(
         any_of(cols$px_reach),
         starts_with("reach_")
      ) %>%
      pivot_longer(
         cols      = starts_with("reach_"),
         names_to  = "reach",
         values_to = "is_reach"
      ) %>%
      mutate(
         reach    = str_replace(reach, "^reach_", ""),
         reach    = case_when(
            reach == "clinical" ~ "1",
            reach == "online" ~ "2",
            reach == "index_testing" ~ "3",
            reach == "index" ~ "3",
            reach == "ssnt" ~ "4",
            reach == "venue" ~ "5",
            TRUE ~ reach
         ),
         is_reach = coalesce(keep_code(is_reach), "0"),
      ) %>%
      filter(is_reach == 1) %>%
      select(any_of(cols$px_reach))

   data$px_other_service <- hts %>%
      select(
         any_of(cols$px_other_service),
         starts_with("service_")
      ) %>%
      select(-service_type) %>%
      pivot_longer(
         cols      = starts_with("service_"),
         names_to  = "service",
         values_to = "given"
      ) %>%
      mutate(
         other_service = case_when(
            service == "service_condoms" ~ given,
            service == "service_lubes" ~ given,
            TRUE ~ NA_character_
         ),
         given         = if_else(!is.na(other_service), "1_Yes", given, given),
         service       = str_replace(service, "^service_", ""),
         service       = case_when(
            service == "hiv_101" ~ "1013",
            service == "iec_mats" ~ "1004",
            service == "risk_counsel" ~ "1002",
            service == "prep_refer" ~ "5001",
            service == "ssnt_offer" ~ "5002",
            service == "ssnt_accept" ~ "5003",
            service == "condoms" ~ "2001",
            service == "lubes" ~ "2002",
            TRUE ~ service
         ),
         given         = coalesce(keep_code(given), "0"),
      ) %>%
      filter(given == 1) %>%
      select(any_of(cols$px_other_service))

   data$px_test_refuse <- hts %>%
      select(
         any_of(cols$px_test_refuse),
         starts_with("test_refuse_")
      ) %>%
      pivot_longer(
         cols      = starts_with("test_refuse_"),
         names_to  = "reason",
         values_to = "is_reason"
      ) %>%
      mutate(
         reason_other = if_else(str_detect(reason, "other_text$"), is_reason, NA_character_),
         is_reason    = if_else(!is.na(reason_other), "1_Yes", is_reason, is_reason),
         reason       = str_replace(reason, "^test_refuse_", ""),
         reason       = str_replace(reason, "_text$", ""),
         reason       = case_when(
            reason == "other" ~ "8888",
            TRUE ~ reason
         ),
         is_reason    = coalesce(keep_code(is_reason), "0"),
      ) %>%
      filter(is_reason == 1) %>%
      select(any_of(cols$px_test_refuse))

   data$px_remarks <- hts %>%
      select(
         any_of(cols$px_remarks),
         clinic_notes,
         counsel_notes,
         symptoms
      ) %>%
      pivot_longer(
         cols      = c(
            clinic_notes,
            counsel_notes,
            symptoms
         ),
         names_to  = "remark_type",
         values_to = "remarks"
      ) %>%
      mutate(
         remark_type = case_when(
            remark_type == "clinic_notes" ~ "1",
            remark_type == "counsel_notes" ~ "2",
            remark_type == "symptoms" ~ "10",
            TRUE ~ remark_type
         ),
      ) %>%
      select(any_of(cols$px_remarks))

   log_info("Finalizing upload schema.")
   schema <- list()
   for (table in tables) {
      schema[[table]] <- list(
         name = table,
         pk   = pks[[table]],
         data = data[[table]] %>%
            filter(if_all(pks[[table]], ~!is.na(.)))
      )
   }

   existing <- local({
      conn <- connect('ohasis-live')
      on.exit(dbDisconnect(conn), add = TRUE)
      QB$new(conn)$select('patient_id')$from('ohasis.patients')$whereIn('patient_id', schema$patients$data$patient_id)$get(format = 'Arrow')
   })

   schema$patients$data %<>%
      anti_join(existing, join_by(patient_id))

   log_success("Done!")
   return(schema)
}

# convert processed hts data into harp_dx registry rows, using the harp_dx step
# functions so the output stays in line with the official pipeline
# hts_data : process_hts() output; may carry hts_rec (matched form of a confirmatory record)
# lab_cd4  : optional central_id, cd4_date, cd4_result (same shape as harp_dx/lab_cd4.sql)
convert_dx <- function(hts_data, yr, mo, lab_cd4 = NULL) {
   if (missing(yr)) {
      yr <- as.numeric(format(Sys.time(), "%Y"))
   }
   if (missing(mo)) {
      mo <- as.numeric(format(Sys.time(), "%m"))
   }

   params <- list(
      yr           = as.numeric(yr),
      mo           = as.numeric(mo),
      p10y         = as.numeric(yr) - 10,
      latest_idnum = NA_integer_ # keep idnum as found, never generate new ones
   )

   # harp_dx step functions (definitions only), kept off the global env
   dx_steps <- new.env(parent = globalenv())
   sys.source("src/official/harp_dx/02_data_hts_tst_pos.R", envir = dx_steps)

   # same correction tables as flow_corr()
   corr <- local({
      con <- connect('mariadb-lw')
      on.exit(dbDisconnect(con), add = TRUE)
      list(
         corr_classd     = QB$new(con)$from("harp_dx.corr_classd")$get(),
         label_values    = QB$new(con)$from("nhsss_stata.label_values")$where("system", "harp_dx")$get(),
         label_variables = QB$new(con)$from("nhsss_stata.label_variables")$where("system", "harp_dx")$get()
      )
   })

   if (is.null(lab_cd4)) {
      lab_cd4 <- tibble(central_id = character(), cd4_date = as.Date(character()), cd4_result = character())
   }

   # the confirmatory record may carry a separate matched form (hts_rec)
   if (!"hts_rec" %in% names(hts_data)) {
      hts_data$hts_rec <- NA_character_
   }

   # same as the tail of harp_dx clean_data(), after forms are matched to confirmatory records
   data <- hts_data %>%
      mutate(hts_rec = coalesce(hts_rec, rec_id)) %>%
      # lake timestamps are local time; clean_dates() would take the date in UTC
      mutate(across(where(is.POSIXct), ~as.Date(., tz = ""))) %>%
      mutate(
         across(
            c(first, middle, last, suffix, patient_code, uic, philhealth_no, philsys_id, client_mobile, client_email),
            ~clean_pii(.)
         )
      ) %>%
      clean_dates() %>%
      get_latest_pii(
         "central_id",
         c(
            "first",
            "middle",
            "last",
            "suffix",
            "birthdate",
            "sex",
            "uic",
            "philhealth_no",
            "self_ident",
            "self_ident_other",
            "philsys_id",
            "civil_status",
            "nationality",
            "educ_level",
            "curr_reg",
            "curr_prov",
            "curr_munc",
            "perm_reg",
            "perm_prov",
            "perm_munc",
            "birth_reg",
            "birth_prov",
            "birth_munc",
            "client_mobile",
            "client_email"
         )
      ) %>%
      rename(
         blood_extract_date    = date_collect,
         specimen_receipt_date = date_receive,
         confirm_date          = date_confirm,
      ) %>%
      mutate(
         # month of labcode/date received
         lab_month      = coalesce(
            str_extract(confirm_code, "[A-Z]+([0-9][0-9])-([0-9][0-9])", 2),
            stri_pad_left(month(specimen_receipt_date), 2, "0")
         ),

         # year of labcode/date received
         lab_year       = coalesce(
            stri_c("20", str_extract(confirm_code, "[A-Z]+([0-9][0-9])-([0-9][0-9])", 1)),
            stri_pad_left(year(specimen_receipt_date), 4, "0")
         ),

         # date variables
         visit_date     = record_date,

         # date var for keeping
         report_date    = as.Date(coalesce(specimen_receipt_date, visit_date)),

         # name
         standard_first = stri_trans_general(first, "latin-ascii"),
         name           = str_squish(stri_c(last, ", ", first, " ", middle, " ", suffix)),

         # Permanent
         perm_prov      = if_else(str_left(perm_reg, 2) == "99", "9999900000", perm_prov, perm_prov),
         perm_munc      = if_else(str_left(perm_reg, 2) == "99", "9999999000", perm_munc, perm_munc),
         use_curr       = if_else(
            condition = !is.na(curr_munc) & (is.na(perm_munc) | str_left(perm_munc, 2) == "99"),
            true      = 1,
            false     = 0
         ),
         perm_reg       = if_else(
            condition = use_curr == 1,
            true      = curr_reg,
            false     = perm_reg
         ),
         perm_prov      = if_else(
            condition = use_curr == 1,
            true      = curr_prov,
            false     = perm_prov
         ),
         perm_munc      = if_else(
            condition = use_curr == 1,
            true      = curr_munc,
            false     = perm_munc
         ),

         # Age
         age            = coalesce(age, age_mo / 12),
         age_dta        = calc_age(birthdate, visit_date),

         form_sort      = if_else(rec_id == hts_rec, 1, 9999, 9999)
      ) %>%
      rename(country_code = nationality) %>%
      left_join(
         y  = ohasis$ref_country %>%
            select(country_code, nationality = country_name),
         by = join_by(country_code)
      ) %>%
      relocate(nationality, .before = country_code) %>%
      select(-country_code)

   data <- dx_steps$prioritize_reports(data)
   data <- dx_steps$get_cd4(data, lab_cd4)
   data <- dx_steps$standardize_data(data, params)
   data <- dx_steps$tag_mot(data, params)
   data <- dx_steps$tag_class(data, corr)
   data <- dx_steps$convert_faci_addr(data)
   data <- dx_steps$final_conversion(data)
   data <- dx_steps$label_stata(data, corr$label_values, corr$label_variables)

   # final type fixes & nodata_hiv_stage, same as harp_dx append_data()
   data %<>%
      mutate(
         confirm_date        = coalesce(confirm_date, as.Date(t3_date)),
         who_staging         = as.integer(who_staging),
         baseline_cd4_result = as.integer(baseline_cd4_result),
      ) %>%
      mutate(across(where(is.labelled), ~to_character(.))) %>%
      mutate(
         nodata_hiv_stage = if_else(
            is.na(ahd) &
               is.na(baseline_cd4) &
               ((coalesce(description_symptoms, "") == "" & clinicalpicture == 1) | is.na(clinicalpicture)) &
               is.na(tbpatient1) &
               is.na(who_staging) &
               class2022 == "HIV",
            1,
            0,
            0
         )
      )

   return(data)
}

changes_dx_v_hts <- function(rec_ids, yr, mo) {
   dx   <- stri_c("harp_dx.reg_", yr, stri_pad_left(mo, 2, "0"))
   con  <- connect("mariadb-lw")
   on.exit(dbDisconnect(con), add = TRUE)
   forms <- lapply(c("form_hts", "form_a", "form_cfbs"), function(form) {
      query <- form_query(con, form)
      if (form == "form_cfbs") {
         do.call(query$select, list(stri_c(form, ".*")))
      }
      query$selectRaw(stri_c("coalesce(id.central_id, ", form, ".patient_id) as central_id"))
      query$leftJoin("ohasis_lake.id_registry as id", stri_c(form, ".patient_id"), "=", "id.patient_id")
      query$whereIn(stri_c(form, ".rec_id"), rec_ids)
      query$get(format = 'Arrow')
   })
   hts  <- forms[[1]]
   a    <- forms[[2]]
   cfbs <- forms[[3]]

   # cd4 only for the clients being checked, up to the end of the period
   central_ids <- unique(c(hts$central_id, a$central_id, cfbs$central_id))
   max_date    <- as.character(ceiling_date(as.Date(stri_c(yr, "-", stri_pad_left(mo, 2, "0"), "-01")), "month") - 1)
   in_ids      <- stri_c("'", central_ids, "'", collapse = ", ")
   # lab_wide has no patient_id, so link through px_demographics (same as harp_dx/lab_cd4.sql)
   cd4         <- dbGetQuery(con, glue(r"(
      select toString(coalesce(id.central_id, pii.patient_id)) as central_id,
             cd4.lab_cd4_date as cd4_date,
             cd4.lab_cd4_result as cd4_result
      from ohasis_lake.lab_wide as cd4
               join ohasis_lake.px_demographics as pii on cd4.rec_id = pii.rec_id
               left join ohasis_lake.id_registry as id on pii.patient_id = id.patient_id
      where cd4.deleted_at is null
        and cd4.lab_cd4_date is not null
        and cd4.lab_cd4_result is not null
        and date(cd4.lab_cd4_date) <= '{max_date}'
        and coalesce(id.central_id, pii.patient_id) in ({in_ids})
   )"))

   # registry rows matched either as the confirmatory record or as its form
   dx <- QB$new(con)$
      from(dx)$
      where(function(query = QB$new(con)) {
         query$whereIn("rec_id", rec_ids, "or")
         query$whereIn("hts_rec", rec_ids, "or")
         query$whereNested
      })$
      get(format = 'Arrow')

   # harp_dx takes t0_result as-is from the confirmatory data, not recoded by process_hts()
   raw_t0  <- bind_rows(
      select(hts, rec_id, any_of("t0_result")),
      select(a, rec_id, any_of("t0_result")),
      select(cfbs, rec_id, any_of(c(t0_result = "test_result")))
   ) %>%
      distinct(rec_id, .keep_all = TRUE)
   records <- process_hts(hts, a, cfbs) %>%
      select(-t0_result) %>%
      left_join(raw_t0, join_by(rec_id))
   convert <- convert_dx(records, yr, mo, cd4)

   variables <- dx %>%
      summary.default() %>%
      as.data.frame() %>%
      pivot_wider(names_from = Var2, values_from = Freq) %>%
      mutate(
         format = case_when(
            Class == "Date" ~ "Date",
            TRUE ~ Mode
         )
      )

   check <- convert %>%
      select(-idnum) %>%
      left_join(
         y  = dx %>%
            select(rec_id, idnum),
         by = join_by(rec_id)
      ) %>%
      mutate_all(as.character) %>%
      pivot_longer(
         cols      = !matches("rec_id"),
         values_to = "new_value",
         names_to  = "variable",
      ) %>%
      right_join(
         y  = dx %>%
            mutate(
               rec_id = coalesce(hts_rec, rec_id)
            ) %>%
            mutate_all(as.character) %>%
            pivot_longer(
               cols      = !matches("rec_id"),
               values_to = "old_value",
               names_to  = "variable",
            ),
         by = join_by(rec_id, variable)
      ) %>%
      mutate(
         period = stri_c(yr, ".", stri_pad_left(mo, 2, "0")),
      ) %>%
      left_join(
         y  = variables %>%
            select(variable = Var1, format),
         by = join_by(variable)
      ) %>%
      left_join(
         y  = dx %>%
            mutate(
               rec_id = coalesce(hts_rec, rec_id)
            ) %>%
            select(rec_id, idnum),
         by = join_by(rec_id)
      ) %>%
      select(
         period,
         idnum,
         variable,
         old_value,
         new_value,
         format
      ) %>%
      filter(coalesce(old_value, "") != coalesce(new_value, "")) %>%
      mutate(
         new_value = coalesce(new_value, "NULL")
      ) %>%
      # year & month in the registry are each record's original report period
      filter(!(variable %in% c('final_interpretation', 'confirm_result', 'confirm_remarks', 'year', 'month')))

   return(check)
}
