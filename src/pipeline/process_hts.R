get_hts <- function(min, max, faci_ids) {

   read_forms <- function(min, max, faci_ids) {
      con   <- connect('mariadb-lw')
      forms <- QB$new(con)
      forms$select(
         "form_hts.*",
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
      forms$from("ohasis_warehouse.form_hts")
      forms$leftJoin("ohasis_lake.px_hiv_confirmatory as confirm", "form_hts.rec_id", "=", "confirm.rec_id")
      forms$leftJoin("ohasis_lake.px_hiv_testing as test", "form_hts.rec_id", "=", "test.rec_id")
      forms$where(function(query = QB$new(con)) {
         query$whereBetween('record_date', c(min, max), "or")
         query$whereBetween('date_confirm', c(min, max), "or")
         query$whereBetween('test.t0_date', c(min, max), "or")
         query$whereBetween('test.t1_date', c(min, max), "or")
         query$whereBetween('test.t2_date', c(min, max), "or")
         query$whereBetween('test.t3_date', c(min, max), "or")
         query$whereNested
      })

      if (!missing(faci_ids)) {
         forms$where(function(query = QB$new(con)) {
            query$whereIn("faci_id", faci_ids, 'or')
            query$whereIn("service_faci", faci_ids, 'or')
            query$whereNested
         })
      }

      form_hts <- forms$get()

      forms <- QB$new(con)
      forms$select(
         "form_a.*",
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
      forms$from("ohasis_warehouse.form_a")
      forms$leftJoin("ohasis_lake.px_hiv_confirmatory as confirm", "form_a.rec_id", "=", "confirm.rec_id")
      forms$leftJoin("ohasis_lake.px_hiv_testing as test", "form_a.rec_id", "=", "test.rec_id")
      forms$where(function(query = QB$new(con)) {
         query$whereBetween('record_date', c(min, max), "or")
         query$whereBetween('date_confirm', c(min, max), "or")
         query$whereBetween('test.t0_date', c(min, max), "or")
         query$whereBetween('test.t1_date', c(min, max), "or")
         query$whereBetween('test.t2_date', c(min, max), "or")
         query$whereBetween('test.t3_date', c(min, max), "or")
         query$whereNested
      })

      if (!missing(faci_ids)) {
         forms$where(function(query = QB$new(con)) {
            query$whereIn("faci_id", faci_ids, 'or')
            query$whereIn("service_faci", faci_ids, 'or')
            query$whereNested
         })
      }

      form_a <- forms$get()

      forms <- QB$new(con)
      forms$from("ohasis_warehouse.form_cfbs")
      forms$where(function(query = QB$new(con)) {
         query$whereBetween('record_date', c(min, max), "or")
         query$whereBetween('test_date', c(min, max), "or")
         query$whereNested
      })

      if (!missing(faci_ids)) {
         forms$where(function(query = QB$new(con)) {
            query$whereIn("faci_id", faci_ids, 'or')
            query$whereIn("service_faci", faci_ids, 'or')
            query$whereNested
         })
      }

      form_cfbs <- forms$get()
      dbDisconnect(con)
      return(list(hts = form_hts, a = form_a, cfbs = form_cfbs))
   }


   starts <- seq(as.Date(min), as.Date(max), by = "1 month")
   ends   <- sapply(starts, function(date) date %m+% months(1) %m-% days(1), simplify = FALSE)

   periods                       <- purrr::map2(lapply(starts, as.character), lapply(ends, as.character), list)
   periods[[length(periods)]][2] <- max

   hts <- lapply(periods, function(period) {
      log_info(r"({green(period[[1]])} to {green(period[[2]])})")
      return(read_forms(period[[1]], period[[2]], faci_ids))
   })

   hts_all  <- purrr::flatten(hts)
   form_hts <- hts_all[names(hts_all) == "hts"] %>%
      bind_rows() %>%
      distinct(rec_id, .keep_all = TRUE)

   form_a <- hts_all[names(hts_all) == "a"] %>%
      bind_rows() %>%
      distinct(rec_id, .keep_all = TRUE)

   form_cfbs <- hts_all[names(hts_all) == "cfbs"] %>%
      bind_rows() %>%
      distinct(rec_id, .keep_all = TRUE)

   return(list(hts = form_hts, a = form_a, cfbs = form_cfbs))
}

# process hts data
process_hts <- function(form_hts = data.frame(), form_a = data.frame(), form_cfbs = data.frame(), testing = data.frame()) {
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
            rename_at(
               .vars = vars(starts_with("risk_")),
               ~stri_replace_first_fixed(., "risk_", "expose_")
            )
      ) %>%
      distinct(rec_id, .keep_all = TRUE)

   if (nrow(testing) > 0) {
      test_same <- intersect(names(testing), names(hts))
      test_diff <- c('rec_id', setdiff(names(testing), names(hts)))
      hts %<>%
         select(-starts_with("t0")) %>%
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
         src = tolower(stri_replace_all_fixed(src, " ", ""))
      ) %>%
      # test information
      mutate_at(
         .vars = vars(
            t0_result,
            t1_result,
            t2_result,
            t3_result,
            confirm_result,
            modality,
            screen_agreed
         ),
         ~keep_code(.)
      ) %>%
      # results
      mutate(
         hts_date        = case_when(
            t0_date >= -25567 & interval(record_date, t0_date) / years(1) <= -2 ~ as.Date(record_date),
            t0_date >= -25567 & interval(record_date, t0_date) / years(1) > -2 ~ as.Date(t0_date),
            !is.na(date_collect) ~ as.Date(date_collect),
            t1_date < record_date ~ as.Date(t1_date),
            TRUE ~ record_date
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
            is.na(screen_agreed) & is.na(hts_result) ~ "REACH",
            confirm_result != 4 & is.na(modality) ~ "FBT",
            src %in% c("a2011", "a2014", "2017") ~ "FBT",
            src == "cfbs2020" & !is.na(hts_result) ~ "CBS",
            src == "hts2021" &
               is.na(modality) &
               !is.na(hts_result) ~ "FBT",
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
            !(hts_modality %in% c("reach", "(no data)")) ~ 1,
            hts_result != "(no data)" ~ 1,
            TRUE ~ 0
         ),
         hts_client_type = case_when(
            str_left(client_type, 1) == "1" ~ "Inpatient",
            hts_modality == "ST" ~ "ST",
            hts_modality %in% c("CBS", "FBS") ~ "CBS",
            str_left(client_type, 1) == "3" ~ "CBS",
            str_left(client_type, 1) == "7" ~ "PDL",
            str_left(client_type, 1) == "2" ~ "Walk-in",
            str_left(client_type, 1) == "4" ~ "Walk-in",
            TRUE ~ "Walk-in"
         )
      )

   log_info("Tagging risks.")
   data <- hts %>%
      # risk information
      mutate_at(
         .vars = vars(starts_with("expose_", ignore.case = FALSE) & !contains("date")),
         ~as.integer(keep_code(.))
      ) %>%
      mutate(
         risk_motherhashiv     = case_when(
            expose_hiv_mother %in% c(1, 2) ~ "yes",
            expose_hiv_mother == 0 ~ "no",
            TRUE ~ "(no data)"
         ),

         # sex with female
         recent_sexwithf       = floor(interval(expose_sex_f_av_date, record_date) / months(1)),
         recent_sexwithf       = case_when(
            recent_sexwithf <= 1 ~ "p01m",
            recent_sexwithf <= 3 ~ "p03m",
            recent_sexwithf <= 6 ~ "p06m",
            recent_sexwithf <= 12 ~ "p12m",
            yr_last_f == year(hts_date) ~ "p12m",
            src == "cfbs2020" &
               (recent_sexwithf > 12 | is.na(recent_sexwithf)) &
               num_f_partner > 0 ~ "p12m",
            src != "cfbs2020" &
               (recent_sexwithf > 12 | is.na(recent_sexwithf)) &
               num_f_partner > 0 ~ "beyond_p12m",
            recent_sexwithf > 12 ~ "beyond_p12m",
            yr_last_f != year(hts_date) ~ "beyond_p12m",
            num_f_partner == 0 ~ "none",
            TRUE ~ "(no data)"
         ),
         recent_sexwithf_nocdm = case_when(
            src == "hts2021" ~ floor(interval(expose_sex_f_av_nocondom_date, record_date) / months(1)),
            src == "cfbs2020" ~ floor(interval(expose_condomless_vaginal_date, record_date) / months(1)),
         ),
         recent_sexwithf_nocdm = case_when(
            recent_sexwithf_nocdm <= 1 ~ "p01m",
            recent_sexwithf_nocdm <= 3 ~ "p03m",
            recent_sexwithf_nocdm <= 6 ~ "p06m",
            recent_sexwithf_nocdm <= 12 ~ "p12m",
            recent_sexwithf_nocdm > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         # consolidate both categories
         recent_sexwithf_c     = case_when(
            recent_sexwithf == "p01m" | recent_sexwithf_nocdm == "p01m" ~ "p01m",
            recent_sexwithf == "p03m" | recent_sexwithf_nocdm == "p03m" ~ "p03m",
            recent_sexwithf == "p06m" | recent_sexwithf_nocdm == "p06m" ~ "p06m",
            recent_sexwithf == "p12m" | recent_sexwithf_nocdm == "p12m" ~ "p12m",
            recent_sexwithf == "beyond_p12m" | recent_sexwithf_nocdm == "beyond_p12m" ~ "beyond_p12m",
            recent_sexwithf == "none" | recent_sexwithf_nocdm == "none" ~ "none",
            recent_sexwithf == "(no data)" & recent_sexwithf_nocdm == "(no data)" ~ "(no data)",
         ),

         risk_sexwithf         = case_when(
            # form a
            src %in% c("a2011", "a2014", "2017") & expose_sex_f_nocondom == 0 ~ "none",
            src %in% c("a2011", "a2014", "2017") & expose_sex_f_nocondom == 1 ~ "yes-p12m",
            src %in% c("a2011", "a2014", "2017") & expose_sex_f_nocondom == 2 ~ "yes-beyond_p12m",
            src %in% c("a2011", "a2014", "2017") & is.na(expose_sex_f_nocondom) ~ "(no data)",

            # hts form
            src == "hts2021" &
               expose_sex_f == 0 &
               (expose_sex_f_av_nocondom == 0 | is.na(expose_sex_f_av_nocondom)) &
               recent_sexwithf_c %in% c("none", "(no data)") ~ "none",
            src == "hts2021" &
               expose_sex_f_av_nocondom == 0 &
               (expose_sex_f == 0 | is.na(expose_sex_f)) &
               recent_sexwithf_c %in% c("none", "(no data)") ~ "none",
            src == "hts2021" &
               (expose_sex_f == 1 | expose_sex_f_av_nocondom == 1) &
               recent_sexwithf_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               (expose_sex_f == 1 | expose_sex_f_av_nocondom == 1) &
               !(recent_sexwithf_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_c),
            src == "hts2021" &
               (expose_sex_f == 0 | expose_sex_f_av_nocondom == 0) &
               !(recent_sexwithf_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_c),
            src == "hts2021" &
               is.na(expose_sex_f) &
               is.na(expose_sex_f_av_nocondom) &
               !(recent_sexwithf_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_c),
            src == "hts2021" &
               is.na(expose_sex_f) &
               is.na(expose_sex_f_av_nocondom) &
               recent_sexwithf_c %in% c("none", "(no data)") ~ recent_sexwithf_c,

            # cfbs form
            src == "cfbs2020" &
               expose_condomless_vaginal == 0 &
               recent_sexwithf_c %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" & !(recent_sexwithf_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_c),
            src == "cfbs2020" & expose_condomless_vaginal == 2 ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               is.na(expose_condomless_vaginal) &
               recent_sexwithf_c %in% c("none", "(no data)") ~ recent_sexwithf_c,
         ),
         risk_sexwithf_nocdm   = case_when(
            # form a
            src %in% c("a2011", "a2014", "2017") & expose_sex_f_nocondom == 0 ~ "none",
            src %in% c("a2011", "a2014", "2017") & expose_sex_f_nocondom == 1 ~ "yes-p12m",
            src %in% c("a2011", "a2014", "2017") & expose_sex_f_nocondom == 2 ~ "yes-beyond_p12m",
            src %in% c("a2011", "a2014", "2017") & is.na(expose_sex_f_nocondom) ~ "(no data)",

            # hts form
            src == "hts2021" &
               expose_sex_f_av_nocondom == 0 &
               recent_sexwithf_nocdm %in% c("none", "(no data)") ~ "none",
            src == "hts2021" &
               expose_sex_f_av_nocondom == 1 &
               recent_sexwithf_nocdm %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               expose_sex_f_av_nocondom == 1 &
               !(recent_sexwithf_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_nocdm),
            src == "hts2021" &
               expose_sex_f_av_nocondom == 0 &
               !(recent_sexwithf_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_nocdm),
            src == "hts2021" &
               is.na(expose_sex_f_av_nocondom) &
               !(recent_sexwithf_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_nocdm),
            src == "hts2021" &
               is.na(expose_sex_f_av_nocondom) &
               recent_sexwithf_nocdm %in% c("none", "(no data)") ~ recent_sexwithf_nocdm,

            # cfbs form
            src == "cfbs2020" &
               expose_condomless_vaginal == 0 &
               recent_sexwithf_nocdm %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" & !(recent_sexwithf_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithf_nocdm),
            src == "cfbs2020" & expose_condomless_vaginal == 2 ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               is.na(expose_condomless_vaginal) &
               recent_sexwithf_nocdm %in% c("none", "(no data)") ~ recent_sexwithf_nocdm,
         ),


         # sex with male
         recent_sexwithm       = floor(interval(expose_sex_m_av_date, record_date) / months(1)),
         recent_sexwithm       = case_when(
            recent_sexwithm <= 1 ~ "p01m",
            recent_sexwithm <= 3 ~ "p03m",
            recent_sexwithm <= 6 ~ "p06m",
            recent_sexwithm <= 12 ~ "p12m",
            yr_last_m == year(hts_date) ~ "p12m",
            src == "cfbs2020" &
               (recent_sexwithm > 12 | is.na(recent_sexwithm)) &
               num_m_partner > 0 ~ "p12m",
            src != "cfbs2020" &
               (recent_sexwithm > 12 | is.na(recent_sexwithm)) &
               num_m_partner > 0 ~ "beyond_p12m",
            recent_sexwithm > 12 ~ "beyond_p12m",
            yr_last_m != year(hts_date) ~ "beyond_p12m",
            num_m_partner == 0 ~ "none",
            TRUE ~ "(no data)"
         ),
         recent_sexwithm_nocdm = case_when(
            src == "hts2021" ~ floor(interval(expose_sex_m_av_nocondom_date, record_date) / months(1)),
            src == "cfbs2020" ~ floor(interval(expose_condomless_vaginal_date, record_date) / months(1)),
         ),
         recent_sexwithm_nocdm = case_when(
            recent_sexwithm_nocdm <= 1 ~ "p01m",
            recent_sexwithm_nocdm <= 3 ~ "p03m",
            recent_sexwithm_nocdm <= 6 ~ "p06m",
            recent_sexwithm_nocdm <= 12 ~ "p12m",
            recent_sexwithm_nocdm > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         # consolidate both categories
         recent_sexwithm_c     = case_when(
            recent_sexwithm == "p01m" | recent_sexwithm_nocdm == "p01m" ~ "p01m",
            recent_sexwithm == "p03m" | recent_sexwithm_nocdm == "p03m" ~ "p03m",
            recent_sexwithm == "p06m" | recent_sexwithm_nocdm == "p06m" ~ "p06m",
            recent_sexwithm == "p12m" | recent_sexwithm_nocdm == "p12m" ~ "p12m",
            recent_sexwithm == "beyond_p12m" | recent_sexwithm_nocdm == "beyond_p12m" ~ "beyond_p12m",
            recent_sexwithm == "none" | recent_sexwithm_nocdm == "none" ~ "none",
            recent_sexwithm == "(no data)" & recent_sexwithm_nocdm == "(no data)" ~ "(no data)",
         ),

         risk_sexwithm         = case_when(
            # form a
            src %in% c("a2011", "a2014", "2017") & expose_sex_m_nocondom == 0 ~ "none",
            src %in% c("a2011", "a2014", "2017") & expose_sex_m_nocondom == 1 ~ "yes-p12m",
            src %in% c("a2011", "a2014", "2017") & expose_sex_m_nocondom == 2 ~ "yes-beyond_p12m",
            src %in% c("a2011", "a2014", "2017") & is.na(expose_sex_m_nocondom) ~ "(no data)",

            # hts form
            src == "hts2021" &
               expose_sex_m == 0 &
               (expose_sex_m_av_nocondom == 0 | is.na(expose_sex_m_av_nocondom)) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "none",
            src == "hts2021" &
               expose_sex_m_av_nocondom == 0 &
               (expose_sex_m == 0 | is.na(expose_sex_m)) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "none",
            src == "hts2021" &
               (expose_sex_m == 1 | expose_sex_m_av_nocondom == 1) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               (expose_sex_m == 1 | expose_sex_m_av_nocondom == 1) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            src == "hts2021" &
               (expose_sex_m == 0 | expose_sex_m_av_nocondom == 0) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            src == "hts2021" &
               is.na(expose_sex_m) &
               is.na(expose_sex_m_av_nocondom) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            src == "hts2021" &
               is.na(expose_sex_m) &
               is.na(expose_sex_m_av_nocondom) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ recent_sexwithm_c,

            # cfbs form
            src == "cfbs2020" &
               expose_condomless_anal == 0 &
               (expose_m_sex_oral_anal == 0 | is.na(expose_m_sex_oral_anal)) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" &
               expose_m_sex_oral_anal == 0 &
               (expose_condomless_anal == 0 | is.na(expose_condomless_anal)) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" &
               (expose_condomless_anal == 1 | expose_m_sex_oral_anal == 1) &
               recent_sexwithm_c %in% c("none", "(no data)", "beyond_p12m") ~ "yes-p12m",
            src == "cfbs2020" &
               (expose_condomless_anal == 2 | expose_m_sex_oral_anal == 2) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               (expose_condomless_anal == 2 | expose_m_sex_oral_anal == 2) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            src == "cfbs2020" &
               (expose_condomless_anal == 0 | expose_m_sex_oral_anal == 0) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            src == "cfbs2020" &
               is.na(expose_condomless_anal) &
               is.na(expose_m_sex_oral_anal) &
               !(recent_sexwithm_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_c),
            src == "cfbs2020" &
               is.na(expose_condomless_anal) &
               is.na(expose_m_sex_oral_anal) &
               recent_sexwithm_c %in% c("none", "(no data)") ~ recent_sexwithm_c,
         ),
         risk_sexwithm_nocdm   = case_when(
            # form a
            src %in% c("a2011", "a2014", "2017") & expose_sex_m_nocondom == 0 ~ "none",
            src %in% c("a2011", "a2014", "2017") & expose_sex_m_nocondom == 1 ~ "yes-p12m",
            src %in% c("a2011", "a2014", "2017") & expose_sex_m_nocondom == 2 ~ "yes-beyond_p12m",
            src %in% c("a2011", "a2014", "2017") & is.na(expose_sex_m_nocondom) ~ "(no data)",

            # hts form
            src == "hts2021" &
               expose_sex_m_av_nocondom == 0 &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ "none",
            src == "hts2021" &
               expose_sex_m_av_nocondom == 1 &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               expose_sex_m_av_nocondom == 1 &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            src == "hts2021" &
               expose_sex_m_av_nocondom == 0 &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            src == "hts2021" &
               is.na(expose_sex_m_av_nocondom) &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            src == "hts2021" &
               is.na(expose_sex_m_av_nocondom) &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ recent_sexwithm_nocdm,

            # cfbs form
            src == "cfbs2020" &
               expose_condomless_anal == 0 &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" &
               expose_condomless_anal == 1 &
               recent_sexwithm_nocdm %in% c("none", "(no data)", "beyond_p12m") ~ "yes-p12m",
            src == "cfbs2020" &
               expose_condomless_anal == 2 &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               expose_condomless_anal == 2 &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            src == "cfbs2020" &
               expose_condomless_anal == 0 &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            src == "cfbs2020" &
               is.na(expose_condomless_anal) &
               !(recent_sexwithm_nocdm %in% c("none", "(no data)")) ~ paste0("yes-", recent_sexwithm_nocdm),
            src == "cfbs2020" &
               is.na(expose_condomless_anal) &
               recent_sexwithm_nocdm %in% c("none", "(no data)") ~ recent_sexwithm_nocdm,
         ),

         # paid for sex / sex worker
         recent_payingforsex   = floor(interval(expose_sex_paying_date, record_date) / months(1)),
         recent_payingforsex   = case_when(
            recent_payingforsex <= 1 ~ "p01m",
            recent_payingforsex <= 3 ~ "p03m",
            recent_payingforsex <= 6 ~ "p06m",
            recent_payingforsex <= 12 ~ "p12m",
            recent_payingforsex > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_payingforsex     = case_when(
            src %in% c("a2011", "a2014", "2017") & expose_sex_paying == 1 ~ "yes-p12m",
            src %in% c("a2011", "a2014", "2017") & expose_sex_paying == 2 ~ "yes-beyond_p12m",
            src %in% c("a2011", "a2014", "2017") & expose_sex_paying == 0 ~ "none",
            src %in% c("a2011", "a2014", "2017") & is.na(expose_sex_paying) ~ "(no data)",
            src == "cfbs2020" ~ "(no data)",
            src == "hts2021" &
               expose_sex_paying == 1 &
               !(recent_payingforsex %in% c("none", "(no data)")) ~ paste0("yes-", recent_payingforsex),
            src == "hts2021" &
               expose_sex_paying == 1 &
               recent_payingforsex %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               expose_sex_paying == 0 &
               recent_payingforsex %in% c("none", "(no data)") ~ "none",
            src == "hts2021" & is.na(expose_sex_paying) ~ "(no data)"
         ),

         # paid for sex / sex worker
         recent_paymentforsex  = floor(interval(expose_sex_payment_date, record_date) / months(1)),
         recent_paymentforsex  = case_when(
            recent_paymentforsex <= 1 ~ "p01m",
            recent_paymentforsex <= 3 ~ "p03m",
            recent_paymentforsex <= 6 ~ "p06m",
            recent_paymentforsex <= 12 ~ "p12m",
            recent_paymentforsex > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_paymentforsex    = case_when(
            src %in% c("a2011", "a2014", "2017") & expose_sex_payment == 1 ~ "yes-p12m",
            src %in% c("a2011", "a2014", "2017") & expose_sex_payment == 2 ~ "yes-beyond_p12m",
            src %in% c("a2011", "a2014", "2017") & expose_sex_payment == 0 ~ "none",
            src %in% c("a2011", "a2014", "2017") & is.na(expose_sex_payment) ~ "(no data)",
            src == "hts2021" &
               expose_sex_payment == 1 &
               !(recent_paymentforsex %in% c("none", "(no data)")) ~ paste0("yes-", recent_paymentforsex),
            src == "hts2021" &
               expose_sex_payment == 1 &
               recent_paymentforsex %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               expose_sex_payment == 0 &
               recent_paymentforsex %in% c("none", "(no data)") ~ "none",
            src == "hts2021" & is.na(expose_sex_payment) ~ "(no data)",
            src == "cfbs2020" &
               expose_sex_payment == 2 &
               !(recent_paymentforsex %in% c("none", "(no data)")) ~ paste0("yes-", recent_paymentforsex),
            src == "cfbs2020" &
               expose_sex_payment == 2 &
               recent_paymentforsex %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               expose_sex_payment == 1 &
               !(recent_paymentforsex %in% c("none", "(no data)", "beyond_p12m")) ~ paste0("yes-", recent_paymentforsex),
            src == "cfbs2020" &
               expose_sex_payment == 1 &
               recent_paymentforsex %in% c("none", "(no data)", "beyond_p12m") ~ "yes-p12m",
            src == "cfbs2020" &
               expose_sex_payment == 0 &
               recent_paymentforsex %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" &
               is.na(expose_sex_payment) &
               recent_paymentforsex == "(no data)" ~ "(no data)",
         ),

         # sex w/ someone who has HIV
         recent_sexwithhiv     = floor(interval(expose_sex_hiv_date, record_date) / months(1)),
         recent_sexwithhiv     = case_when(
            recent_sexwithhiv <= 1 ~ "p01m",
            recent_sexwithhiv <= 3 ~ "p03m",
            recent_sexwithhiv <= 6 ~ "p06m",
            recent_sexwithhiv <= 12 ~ "p12m",
            recent_sexwithhiv > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_sexwithhiv       = case_when(
            src %in% c("a2011", "a2014", "2017") & expose_sex_hiv == 1 ~ "yes-p12m",
            src %in% c("a2011", "a2014", "2017") & expose_sex_hiv == 2 ~ "yes-beyond_p12m",
            src %in% c("a2011", "a2014", "2017") & expose_sex_hiv == 0 ~ "none",
            src %in% c("a2011", "a2014", "2017") & is.na(expose_sex_hiv) ~ "(no data)",
            src == "hts2021" ~ "(no data)",
            src == "cfbs2020" &
               expose_sex_hiv == 2 &
               !(recent_payingforsex %in% c("none", "(no data)")) ~ paste0("yes-", recent_payingforsex),
            src == "cfbs2020" &
               expose_sex_hiv == 2 &
               recent_payingforsex %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               expose_sex_hiv == 0 &
               recent_payingforsex %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" & is.na(expose_sex_hiv) ~ "(no data)"
         ),

         # shared injects / injecting drugs
         recent_injectdrug     = floor(interval(expose_drug_inject_date, record_date) / months(1)),
         recent_injectdrug     = case_when(
            recent_injectdrug <= 1 ~ "p01m",
            recent_injectdrug <= 3 ~ "p03m",
            recent_injectdrug <= 6 ~ "p06m",
            recent_injectdrug <= 12 ~ "p12m",
            recent_injectdrug > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         recent_injectshare    = floor(interval(expose_needle_share_date, record_date) / months(1)),
         recent_injectshare    = case_when(
            recent_injectshare <= 1 ~ "p01m",
            recent_injectshare <= 3 ~ "p03m",
            recent_injectshare <= 6 ~ "p06m",
            recent_injectshare <= 12 ~ "p12m",
            recent_injectshare > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         # consolidate both categories
         recent_injectdrug_c   = case_when(
            recent_injectshare == "p01m" | recent_injectdrug == "p01m" ~ "p01m",
            recent_injectshare == "p03m" | recent_injectdrug == "p03m" ~ "p03m",
            recent_injectshare == "p06m" | recent_injectdrug == "p06m" ~ "p06m",
            recent_injectshare == "p12m" | recent_injectdrug == "p12m" ~ "p12m",
            recent_injectshare == "beyond_p12m" | recent_injectdrug == "beyond_p12m" ~ "beyond_p12m",
            recent_injectshare == "none" | recent_injectdrug == "none" ~ "none",
            recent_injectshare == "(no data)" & recent_injectdrug == "(no data)" ~ "(no data)",
         ),

         risk_injectdrug       = case_when(
            # form a
            src %in% c("a2011", "a2014", "2017") & expose_drug_inject == 1 ~ "yes-p12m",
            src %in% c("a2011", "a2014", "2017") & expose_drug_inject == 2 ~ "yes-beyond_p12m",
            src %in% c("a2011", "a2014", "2017") & expose_drug_inject == 0 ~ "none",
            src %in% c("a2011", "a2014", "2017") & is.na(expose_drug_inject) ~ "(no data)",

            # hts form
            src == "hts2021" &
               expose_drug_inject == 1 &
               !(recent_injectdrug_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_injectdrug_c),
            src == "hts2021" &
               expose_drug_inject == 1 &
               recent_injectdrug_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               expose_drug_inject == 0 &
               recent_injectdrug_c %in% c("none", "(no data)") ~ "none",
            src == "hts2021" & is.na(expose_drug_inject) ~ "(no data)",

            # cfbs form
            src == "cfbs2020" &
               expose_drug_inject == 2 &
               (expose_needle_share %in% c(0, 2) | is.na(expose_needle_share)) &
               !(recent_injectdrug_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_injectdrug_c),
            src == "cfbs2020" &
               expose_needle_share == 2 &
               (expose_drug_inject %in% c(0, 2) | is.na(expose_drug_inject)) &
               !(recent_injectdrug_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_injectdrug_c),
            src == "cfbs2020" &
               expose_drug_inject == 2 &
               (expose_needle_share %in% c(0, 2) | is.na(expose_needle_share)) &
               recent_injectdrug_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               expose_needle_share == 2 &
               (expose_drug_inject %in% c(0, 2) | is.na(expose_drug_inject)) &
               recent_injectdrug_c %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               expose_drug_inject == 1 &
               !(recent_injectdrug_c %in% c("none", "(no data)", "beyond_p12m")) ~ paste0("yes-", recent_injectdrug_c),
            src == "cfbs2020" &
               expose_drug_inject == 1 &
               recent_injectdrug_c %in% c("none", "(no data)", "beyond_p12m") ~ "yes-p12m",
            src == "cfbs2020" &
               expose_drug_inject == 0 &
               !(recent_injectdrug_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_injectdrug_c),
            src == "cfbs2020" &
               expose_drug_inject == 0 &
               recent_injectdrug_c %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" &
               is.na(expose_drug_inject) &
               recent_injectdrug_c == "(no data)" ~ "(no data)",
            src == "cfbs2020" &
               is.na(expose_drug_inject) &
               !(recent_injectdrug_c %in% c("none", "(no data)")) ~ paste0("yes-", recent_injectdrug_c),
         ),

         # occupational exposure
         recent_needlestick    = floor(interval(expose_occupation_date, record_date) / months(1)),
         recent_needlestick    = case_when(
            recent_needlestick <= 1 ~ "p01m",
            recent_needlestick <= 3 ~ "p03m",
            recent_needlestick <= 6 ~ "p06m",
            recent_needlestick <= 12 ~ "p12m",
            recent_needlestick > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_needlestick      = case_when(
            src %in% c("a2011", "a2014", "2017") & expose_occupation == 1 ~ "yes-p12m",
            src %in% c("a2011", "a2014", "2017") & expose_occupation == 2 ~ "yes-beyond_p12m",
            src %in% c("a2011", "a2014", "2017") & expose_occupation == 0 ~ "none",
            src %in% c("a2011", "a2014", "2017") & is.na(expose_occupation) ~ "(no data)",
            src == "cfbs2020" ~ "(no data)",
            src == "hts2021" &
               expose_occupation == 1 &
               !(recent_needlestick %in% c("none", "(no data)")) ~ paste0("yes-", recent_needlestick),
            src == "hts2021" &
               expose_occupation == 1 &
               recent_needlestick %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               expose_occupation == 0 &
               recent_needlestick %in% c("none", "(no data)") ~ "none",
            src == "hts2021" & is.na(expose_occupation) ~ "(no data)"
         ),

         # blood transfusion
         recent_bloodtransfuse = floor(interval(expose_blood_transfuse_date, record_date) / months(1)),
         recent_bloodtransfuse = case_when(
            recent_bloodtransfuse <= 1 ~ "p01m",
            recent_bloodtransfuse <= 3 ~ "p03m",
            recent_bloodtransfuse <= 6 ~ "p06m",
            recent_bloodtransfuse <= 12 ~ "p12m",
            recent_bloodtransfuse > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_bloodtransfuse   = case_when(
            src %in% c("a2011", "a2014", "2017") & expose_blood_transfuse == 1 ~ "yes-p12m",
            src %in% c("a2011", "a2014", "2017") & expose_blood_transfuse == 2 ~ "yes-beyond_p12m",
            src %in% c("a2011", "a2014", "2017") & expose_blood_transfuse == 0 ~ "none",
            src %in% c("a2011", "a2014", "2017") & is.na(expose_blood_transfuse) ~ "(no data)",
            src == "cfbs2020" ~ "(no data)",
            src == "hts2021" &
               expose_blood_transfuse == 1 &
               !(recent_bloodtransfuse %in% c("none", "(no data)")) ~ paste0("yes-", recent_bloodtransfuse),
            src == "hts2021" &
               expose_blood_transfuse == 1 &
               recent_bloodtransfuse %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               expose_blood_transfuse == 0 &
               recent_bloodtransfuse %in% c("none", "(no data)") ~ "none",
            src == "hts2021" & is.na(expose_blood_transfuse) ~ "(no data)"
         ),

         # chemsex & drugs
         recent_illicitdrug    = floor(interval(expose_illicit_drugs_date, record_date) / months(1)),
         recent_illicitdrug    = case_when(
            recent_illicitdrug <= 1 ~ "p01m",
            recent_illicitdrug <= 3 ~ "p03m",
            recent_illicitdrug <= 6 ~ "p06m",
            recent_illicitdrug <= 12 ~ "p12m",
            recent_illicitdrug > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_illicitdrug      = case_when(
            src %in% c("a2011", "a2014", "2017") ~ "(no data)",
            src == "hts2021" ~ "(no data)",
            src == "cfbs2020" &
               expose_illicit_drugs == 2 &
               !(recent_illicitdrug %in% c("none", "(no data)")) ~ paste0("yes-", recent_illicitdrug),
            src == "cfbs2020" &
               expose_illicit_drugs == 2 &
               recent_illicitdrug %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "cfbs2020" &
               expose_illicit_drugs == 0 &
               recent_illicitdrug %in% c("none", "(no data)") ~ "none",
            src == "cfbs2020" & is.na(expose_illicit_drugs) ~ "(no data)"
         ),

         # had sex under influence of drugs
         recent_chemsex        = floor(interval(expose_sex_drugs_date, record_date) / months(1)),
         recent_chemsex        = case_when(
            recent_chemsex <= 1 ~ "p01m",
            recent_chemsex <= 3 ~ "p03m",
            recent_chemsex <= 6 ~ "p06m",
            recent_chemsex <= 12 ~ "p12m",
            recent_chemsex > 12 ~ "beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_chemsex          = case_when(
            src %in% c("a2011", "a2014", "2017") ~ "(no data)",
            src == "cfbs2020" ~ "(no data)",
            src == "hts2021" &
               expose_sex_drugs == 1 &
               !(recent_chemsex %in% c("none", "(no data)")) ~ paste0("yes-", recent_chemsex),
            src == "hts2021" &
               expose_sex_drugs == 1 &
               recent_chemsex %in% c("none", "(no data)") ~ "yes-beyond_p12m",
            src == "hts2021" &
               expose_sex_drugs == 0 &
               recent_chemsex %in% c("none", "(no data)") ~ "none",
            src == "hts2021" & is.na(expose_sex_drugs) ~ "(no data)"
         ),

         # tattoo
         risk_tattoo           = case_when(
            src %in% c("a2011", "a2014", "2017") & expose_tattoo == 1 ~ "yes-p12m",
            src %in% c("a2011", "a2014", "2017") & expose_tattoo == 2 ~ "yes-beyond_p12m",
            src %in% c("a2011", "a2014", "2017") & expose_tattoo == 0 ~ "none",
            src %in% c("a2011", "a2014", "2017") & is.na(expose_tattoo) ~ "(no data)",
            src == "cfbs2020" ~ "(no data)",
            src == "hts2021" ~ "(no data)",
         ),

         # sti
         risk_sti              = case_when(
            src %in% c("a2011", "a2014", "2017") & expose_sti == 1 ~ "yes-p12m",
            src %in% c("a2011", "a2014", "2017") & expose_sti == 2 ~ "yes-beyond_p12m",
            src %in% c("a2011", "a2014", "2017") & expose_sti == 0 ~ "none",
            src %in% c("a2011", "a2014", "2017") & is.na(expose_sti) ~ "(no data)",
            src == "cfbs2020" ~ "(no data)",
            src == "hts2021" ~ "(no data)",
         ),

         # mot from dx processing

         # for mot
         motherisi1            = case_when(
            expose_hiv_mother > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithf              = case_when(
            expose_sex_f > 0 ~ 1,                      # HTS Form
            !is.na(expose_sex_f_av_date) ~ 1,          # HTS Form
            !is.na(expose_sex_f_av_nocondom_date) ~ 1, # HTS Form
            expose_sex_f_nocondom > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithm              = case_when(
            expose_sex_m > 0 ~ 1,                      # HTS Form
            !is.na(expose_sex_m_av_date) ~ 1,          # HTS Form
            !is.na(expose_sex_m_av_nocondom_date) ~ 1, # HTS Form
            expose_sex_m_nocondom > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithpro            = case_when(
            expose_sex_paying > 0 ~ 1,
            TRUE ~ 0
         ),
         regularlya            = case_when(
            expose_sex_payment > 0 ~ 1,
            TRUE ~ 0
         ),
         injectdrug            = case_when(
            expose_drug_inject > 0 ~ 1,
            TRUE ~ 0
         ),
         chemsex               = case_when(
            expose_sex_drugs > 0 ~ 1, # HTS Form
            TRUE ~ 0
         ),
         receivedbt            = case_when(
            expose_blood_transfuse > 0 ~ 1,
            TRUE ~ 0
         ),
         sti                   = case_when(
            expose_sti > 0 ~ 1,
            TRUE ~ 0
         ),
         needlepri1            = case_when(
            expose_occupation > 0 ~ 1,
            TRUE ~ 0
         ),

         p10y                  = hts_date %m-% years(1),
         p10y                  = year(p10y),

         mot                   = 0,
         # m->m only
         mot                   = case_when(
            sex == "1_Male" & expose_sex_m_nocondom == 1 ~ 1,
            sex == "1_Male" & yr_last_m >= p10y ~ 1,
            sex == "1_Male" & year(expose_sex_m_av_date) >= p10y ~ 1,          # HTS Form
            sex == "1_Male" & year(expose_sex_m_av_nocondom_date) >= p10y ~ 1, # HTS Form
            TRUE ~ mot
         ),

         # m->m+f
         mot                   = case_when(
            mot == 1 & expose_sex_f_nocondom == 1 ~ 2,
            mot == 1 & yr_last_f >= p10y ~ 2,
            mot == 1 & year(expose_sex_f_av_date) >= p10y ~ 2,          # HTS Form
            mot == 1 & year(expose_sex_f_av_nocondom_date) >= p10y ~ 2, # HTS Form
            TRUE ~ mot
         ),

         # m->f only
         mot                   = case_when(
            sex == "1_Male" &
               mot == 0 &
               expose_sex_f_nocondom == 1 ~ 3,
            sex == "1_Male" &
               mot == 0 &
               yr_last_f >= p10y ~ 3,
            sex == "1_Male" &
               mot == 0 &
               year(expose_sex_f_av_date) >= p10y ~ 3,          # HTS Form
            sex == "1_Male" &
               mot == 0 &
               year(expose_sex_f_av_nocondom_date) >= p10y ~ 3, # HTS Form
            TRUE ~ mot
         ),

         # f->m
         mot                   = case_when(
            sex == "2_Female" & expose_sex_m_nocondom == 1 ~ 4,
            sex == "2_Female" & yr_last_m >= p10y ~ 4,
            sex == "2_Female" & year(expose_sex_m_av_date) >= p10y ~ 4,          # HTS Form
            sex == "2_Female" & year(expose_sex_m_av_nocondom_date) >= p10y ~ 4, # HTS Form
            TRUE ~ mot
         ),

         # IVDU
         mot                   = case_when(
            expose_drug_inject > 0 & str_left(perm_prov, 4) == "0722" ~ 5,
            TRUE ~ mot
         ),

         # vertical
         mot                   = case_when(
            mot == 0 & motherisi1 == 1 ~ 6,
            TRUE ~ mot
         ),

         # m->m-f hx
         mot                   = case_when(
            sex == "1_Male" &
               mot == 0 &
               num_m_partner > 0 &
               is.na(yr_last_m) ~ 11,
            sex == "1_Male" &
               mot == 0 &
               yr_last_m >= p10y ~ 11,
            sex == "1_Male" &
               mot == 0 &
               !is.na(expose_sex_m_av_date) ~ 11,                           # HTS Form
            sex == "1_Male" &
               mot == 0 &
               !is.na(expose_sex_m_av_nocondom_date) ~ 11,                  # HTS Form
            sex == "1_Male" & mot == 0 & expose_sex_m > 0 ~ 11,             # HTS Form
            TRUE ~ mot
         ),

         # m->m+f hx
         mot                   = case_when(
            mot == 1 & num_f_partner > 0 & is.na(yr_last_f) ~ 21,
            mot == 3 & num_m_partner > 0 & is.na(yr_last_m) ~ 21,
            mot == 11 & num_f_partner > 0 & is.na(yr_last_f) ~ 21,
            mot == 11 & yr_last_f >= p10y ~ 21,
            mot == 11 & !is.na(expose_sex_f_av_date) ~ 21,          # HTS Form,
            mot == 11 & !is.na(expose_sex_f_av_nocondom_date) ~ 21, # HTS Form,
            mot == 11 & expose_sex_f > 0 ~ 21,                      # HTS Form,
            TRUE ~ mot
         ),

         # m->f hx
         mot                   = case_when(
            sex == "1_Male" &
               mot == 0 &
               num_f_partner > 0 &
               is.na(yr_last_f) ~ 31,
            sex == "1_Male" &
               mot == 0 &
               yr_last_f >= p10y ~ 31,
            sex == "1_Male" &
               mot == 0 &
               !is.na(expose_sex_f_av_date) ~ 31,                           # HTS Form,
            sex == "1_Male" &
               mot == 0 &
               !is.na(expose_sex_f_av_nocondom_date) ~ 31,                  # HTS Form,
            sex == "1_Male" & mot == 0 & expose_sex_f > 0 ~ 31,             # HTS Form,
            TRUE ~ mot
         ),

         # f->m hx
         mot                   = case_when(
            sex == "2_Female" &
               mot == 0 &
               num_m_partner > 0 &
               is.na(yr_last_m) ~ 41,
            sex == "2_Female" &
               mot == 0 &
               yr_last_m >= p10y ~ 41,
            sex == "2_Female" &
               mot == 0 &
               !is.na(expose_sex_m_av_date) ~ 41,                    # HTS Form,
            sex == "2_Female" &
               mot == 0 &
               !is.na(expose_sex_m_av_nocondom_date) ~ 41,           # HTS Form,
            sex == "2_Female" & mot == 0 & expose_sex_m > 0 ~ 41,    # HTS Form,
            TRUE ~ mot
         ),

         # IVDU hx
         mot                   = case_when(
            injectdrug > 0 & str_left(perm_prov, 4) == "0722" ~ 51,
            TRUE ~ mot
         ),

         # mtct
         mot                   = case_when(
            mot == 0 & age < 5 ~ 61,
            TRUE ~ mot
         ),

         # all else fails
         mot                   = case_when(
            sex == "1_Male" & mot == 0 & num_m_partner > 0 ~ 1,
            TRUE ~ mot
         ),
         mot                   = case_when(
            mot == 1 & num_f_partner > 0 ~ 2,
            TRUE ~ mot
         ),

         # needlestick
         mot                   = case_when(
            mot == 0 & needlepri1 == 1 ~ 7,
            TRUE ~ mot
         ),

         # transfusion
         mot                   = case_when(
            mot == 0 & receivedbt == 1 ~ 8,
            TRUE ~ mot
         ),

         # no data
         mot                   = case_when(
            mot == 0 ~ 9,
            TRUE ~ mot
         ),

         # f->f
         mot                   = case_when(
            sex == "2_Female" & mot == 0 & num_f_partner > 0 ~ 10,
            sex == "2_Female" &
               mot == 0 &
               !is.na(yr_last_f) > 0 ~ 10,
            sex == "2_Female" &
               mot == 0 &
               !is.na(expose_sex_f_av_date) ~ 10,
            sex == "2_Female" &
               mot == 0 &
               !is.na(expose_sex_f_av_nocondom_date) ~ 10,
            sex == "2_Female" & mot == 0 & expose_sex_f > 0 ~ 10,
            TRUE ~ mot
         ),

         # # clean mot_09
         # mot                  = case_when(
         #    mot %in% c(7, 8) ~ 9,
         #    TRUE ~ mot
         # ),

         # final filtering of mot using risk_*
         mot                   = case_when(
            mot %in% c(7, 8, 9, 10) &
               sex == "1_Male" &
               str_detect(risk_sexwithm, "^yes") &
               str_detect(risk_sexwithf, "^yes") ~ 22,
            mot %in% c(7, 8, 9, 10) &
               sex == "1_Male" &
               str_detect(risk_sexwithm, "^yes") &
               !str_detect(risk_sexwithf, "^yes") ~ 12,
            mot %in% c(7, 8, 9, 10) &
               sex == "1_Male" &
               !str_detect(risk_sexwithm, "^yes") &
               str_detect(risk_sexwithf, "^yes") ~ 32,
            mot %in% c(7, 8, 9, 10) &
               sex == "2_Female" &
               str_detect(risk_sexwithm, "^yes") &
               !str_detect(risk_sexwithf, "^yes") ~ 42,
            mot %in% c(7, 8, 9, 10) & str_detect(risk_injectdrug, "^yes") ~ 52,
            TRUE ~ mot
         ),

         # transmit
         transmit              = case_when(
            mot %in% c(1, 2, 3, 4, 11, 12, 21, 22, 31, 32, 41, 42) ~ "SEX",
            mot %in% c(5, 51, 52) ~ "IVDU",
            mot %in% c(6, 61) ~ "PERINATAL",
            mot %in% c(8, 9, 10) ~ "UNKNOWN",
            mot == 7 ~ "OTHERS",
         ),

         # sexhow
         sexhow                = case_when(
            mot %in% c(1, 11, 12) ~ "HOMOSEXUAL",
            mot %in% c(2, 21, 22) ~ "BISEXUAL",
            mot %in% c(3, 4, 31, 32, 41, 42) ~ "HETEROSEXUAL",
         ),


         mot                   = labelled(
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
               'M->M hx'                 = 21,
               'M->(M+F) hx'             = 31,
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
            grepl("GRINDR", cbs_venue) ~ "GRINDR",
            grepl("GRNDR", cbs_venue) ~ "GRINDR",
            grepl("GRINDER", cbs_venue) ~ "GRINDR",
            grepl("TWITTER", cbs_venue) ~ "TWITTER",
            grepl("FACEBOOK", cbs_venue) ~ "FACEBOOK",
            grepl("MESSENGER", cbs_venue) ~ "FACEBOOK",
            grepl("\\bFB\\b", cbs_venue) ~ "FACEBOOK",
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
         ),
         -starts_with("expose_")
      ) %>%
      left_join(
         y  = hts %>%
            select(
               rec_id,
               starts_with("expose_")
            ),
         by = join_by(rec_id)
      ) %>%
      relocate(any_of(names(hts)), .before = 1)

   log_info("Combining risks.")
   hts_risk <- data %>%
      select(
         rec_id,
         contains("risk", ignore.case = FALSE)
      ) %>%
      pivot_longer(
         cols = contains("risk", ignore.case = FALSE)
      ) %>%
      group_by(rec_id) %>%
      summarise(
         risks = stri_c(collapse = ", ", unique(sort(value)))
      )

   log_info("Finalizing KPs.")
   data %<>%
      left_join(hts_risk, join_by(rec_id)) %>%
      mutate(
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
         kap_pdl     = case_when(
            str_left(client_type, 1) == "7" ~ "PDL",
            str_left(client_type, 1) == "7" ~ "PDL",
         ),
      )

   return(data)
}

convert_hts <- function(hts_data, convert_type = c("nhsss", "name", "code")) {
   data <- hts_data %>%
      mutate(
         use_record_faci = if_else(is.na(service_faci), 1, 0, 0),
         service_faci    = if_else(use_record_faci == 1, faci_id, service_faci),

         perm_prov       = if_else(str_left(perm_reg, 2) == "99", "999900000", perm_prov, perm_prov),
         perm_munc       = if_else(str_left(perm_reg, 2) == "99", "999999000", perm_munc, perm_munc),
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
      "patients",
      "px_record",
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
      "px_remarks"
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
            verbal_consent,
            ofw_station
         ),
         ~keep_code(.)
      ) %>%
      mutate(
         form_id = 'hts2021'
      ) %>%
      rename(
         location_reg  = hiv_service_reg,
         location_reg  = hiv_service_reg,
         location_prov = hiv_service_prov,
         location_munc = hiv_service_munc,
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

   conn <- ohasis$conn("db")

   # primary keys
   log_info("Obtaining {green('Primary Keys')}.")
   pks        <- lapply(tables, function(table) dbGetQuery(conn, glue("show keys from ohasis.{table} where Key_name = 'primary'")))
   pks        <- lapply(pks, function(data) return(data$Column_name))
   names(pks) <- tables


   # columns
   log_info("Obtaining {green('Column Names')}.")
   cols        <- lapply(tables, function(table) dbGetQuery(conn, glue("show columns from ohasis.{table}")))
   cols        <- lapply(cols, function(data) return(data$Field))
   names(cols) <- tables

   dbDisconnect(conn)

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
         reason_other = case_when(
            reason == "test_refuse_condoms" ~ is_reason,
            reason == "test_refuse_lubes" ~ is_reason,
            TRUE ~ NA_character_
         ),
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
         data = data[[table]]
      )
   }

   log_success("Done!")
   return(schema)
}

convert_dx <- function(hts_data, yr, mo) {
   if (missing(yr)) {
      yr <- format(Sys.time(), "%Y")
   }
   if (missing(mo)) {
      mo <- format(Sys.time(), "%m")
   }

   con         <- connect('ohasis-lw')
   corr_classd <- QB$new(con)$from("harp_dx.corr_classd")$get()
   dbDisconnect(con)

   params <- list(
      yr = yr,
      mo = mo
   )


   converted <- hts_data %>%
      mutate_at(
         .vars = vars(first, middle, last, suffix, patient_code, uic, philhealth_no, philsys_id, client_mobile, client_email),
         ~clean_pii(.)
      ) %>%
      mutate_if(
         .predicate = is.POSIXct,
         ~as.Date(.)
      ) %>%
      mutate_if(
         .predicate = is.Date,
         ~if_else(. <= -25567, NA_Date_, ., .)
      ) %>%
      rename(
         blood_extract_date    = date_collect,
         specimen_receipt_date = date_receive,
         confirm_date          = date_confirm,
      ) %>%
      mutate(
         # month of labcode/date received
         lab_month       = coalesce(
            str_extract(confirm_code, "[A-Z]+([0-9][0-9])-([0-9][0-9])", 2),
            stri_pad_left(month(specimen_receipt_date), 2, "0")
         ),

         # year of labcode/date received
         lab_year        = coalesce(
            stri_c("20", str_extract(confirm_code, "[A-Z]+([0-9][0-9])-([0-9][0-9])", 1)),
            stri_pad_left(year(specimen_receipt_date), 4, "0")
         ),

         # date variables
         visit_date      = record_date,

         # date var for keeping
         report_date     = as.Date(stri_c(sep = "-", lab_year, lab_month, "01")),

         # name
         standard_first  = stri_trans_general(first, "latin-ascii"),
         name            = str_squish(stri_c(last, ", ", first, " ", middle, " ", suffix)),

         # Permanent
         perm_prov       = if_else(str_left(perm_reg, 2) == "99", "999900000", perm_prov, perm_prov),
         perm_munc       = if_else(str_left(perm_reg, 2) == "99", "999999000", perm_munc, perm_munc),
         use_curr        = if_else(
            condition = !is.na(curr_munc) & (is.na(perm_munc) | str_left(perm_munc, 2) == "99"),
            true      = 1,
            false     = 0
         ),
         perm_reg        = if_else(
            condition = use_curr == 1,
            true      = curr_reg,
            false     = perm_reg
         ),
         perm_prov       = if_else(
            condition = use_curr == 1,
            true      = curr_prov,
            false     = perm_prov
         ),
         perm_munc       = if_else(
            condition = use_curr == 1,
            true      = curr_munc,
            false     = perm_munc
         ),

         # Age
         age             = coalesce(age, age_mo / 12),
         age_dta         = calc_age(birthdate, visit_date),

         hts_rec         = rec_id,
         form_sort       = if_else(rec_id == hts_rec, 1, 9999, 9999),

         p10y            = year(visit_date %m-% years(10)),
         confirm_remarks = NA_character_
      ) %>%
      rename(
         test_faci     = service_faci,
         test_sub_faci = service_sub_faci,
      ) %>%
      mutate(
         # calculate distance from confirmatory date
         cd4_date                  = NA_Date_,
         cd4_confirm               = NA_integer_,

         # baseline is within 182 days
         baseline_cd4              = NA_integer_,
         idnum                     = NA_integer_,

         # report date
         year                      = params$yr,
         month                     = params$mo,

         # Perm Region (as encoded)
         permonly_reg              = if_else(
            condition = use_curr == 0,
            true      = perm_reg,
            false     = NA_character_
         ),
         permonly_prov             = if_else(
            condition = use_curr == 0,
            true      = perm_prov,
            false     = NA_character_
         ),
         permonly_munc             = if_else(
            condition = use_curr == 0,
            true      = perm_munc,
            false     = NA_character_
         ),

         # tagging vars
         male                      = if_else(
            condition = str_left(sex, 1) == "1",
            true      = 1,
            false     = 0
         ),
         female                    = if_else(
            condition = str_left(sex, 1) == "2",
            true      = 1,
            false     = 0
         ),

         # confirmatory info
         test_done                 = case_when(
            str_detect(toupper(t3_kit), "GEENIUS") ~ "GEENIUS",
            str_detect(toupper(t3_kit), "STAT-PAK") ~ "STAT-PAK",
            str_detect(toupper(t3_kit), "MP DIAGNOSTICS") ~ "WESTERN BLOT",
            age <= 1 ~ "pcr"
         ),
         rhivda_done               = if_else(
            condition = str_left(confirm_type, 1) == "2",
            true      = 1,
            false     = as.numeric(na)
         ),
         sample_source             = substr(specimen_refer_type, 3, 3),

         # demographics
         pxcode                    = str_squish(stri_c(str_left(first, 1), str_left(middle, 1), str_left(last, 1))),
         sex                       = remove_code(stri_trans_toupper(sex)),
         self_identity             = remove_code(stri_trans_toupper(self_ident)),
         self_identity             = case_when(
            SELF_IDENTITY == "OTHER" ~ "OTHERS",
            SELF_IDENTITY == "MAN" ~ "MALE",
            SELF_IDENTITY == "WOMAN" ~ "FEMALE",
            SELF_IDENTITY == "MALE" ~ "MALE",
            SELF_IDENTITY == "FEMALE" ~ "FEMALE",
            TRUE ~ self_identity
         ),
         self_identity_other       = stri_trans_toupper(self_ident_other),
         self_identity_other_sieve = str_replace_all(self_identity_other, "[^[:alnum:]]", ""),

         civil_status              = stri_trans_toupper(civil_status),
         nationalit                = case_when(
            toupper(nationality) == "PHILIPPINES" ~ "FILIPINO",
            toupper(nationality) != "PHILIPPINES" ~ "NON-FILIPINO",
            TRUE ~ "UNKNOWN"
         ),
         current_school_level      = if_else(
            condition = str_left(is_student, 1) == "1",
            true      = educ_level,
            false     = NA_character_
         ),

         # occupation
         curr_work                 = if_else(
            condition = str_left(is_employed, 1) == "1",
            true      = stri_trans_toupper(work),
            false     = NA_character_
         ),
         prev_work                 = if_else(
            condition = str_left(is_employed, 1) == "0" | is.na(is_employed),
            true      = stri_trans_toupper(work),
            false     = NA_character_
         ),

         # clinical pic
         who_staging               = as.integer(keep_code(who_class)),
         other_reason_test         = stri_trans_toupper(test_reason_other_text),

         clinical_pic              = case_when(
            str_left(clinical_pic, 1) == "1" ~ "0_Asymptomatic",
            str_left(clinical_pic, 1) == "2" ~ "1_Symptomatic",
         ),

         ofw_station               = case_when(
            str_left(ofw_station, 1) == "1" ~ "1_On ship",
            str_left(ofw_station, 1) == "2" ~ "2_Land",
         ),

         refer_type                = case_when(
            str_left(refer_type, 1) == "1" ~ "1",
            str_left(refer_type, 1) == "2" ~ "1",
         )
      ) %>%
      # exposure history
      mutate_at(
         .vars = vars(starts_with("expose_") & !contains("date")),
         ~if_else(
            condition = !is.na(.),
            true      = str_left(., 1),
            false     = NA_character_,
         ) %>% as.integer()
      ) %>%
      # medical history
      mutate_at(
         .vars = vars(starts_with("med_")),
         ~if_else(
            condition = !is.na(.),
            true      = str_left(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # test reason
      mutate_at(
         .vars = vars(starts_with("test_reason") & !matches("_OTHER")),
         ~if_else(
            condition = !is.na(.),
            true      = str_left(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # mode of reach (HTS)
      mutate_at(
         .vars = vars(starts_with("reach_")),
         ~if_else(
            condition = !is.na(.),
            true      = str_left(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # mode of reach (HTS)
      mutate_at(
         .vars = vars(starts_with("refer")),
         ~if_else(
            condition = !is.na(.),
            true      = str_left(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # services provided (HTS)
      mutate_at(
         .vars = vars(starts_with("service_")),
         ~if_else(
            condition = !is.na(.),
            true      = str_left(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      generate_gender_identity(sex, self_ident, self_ident_other, gender_identity) %>%
      # mode of transmission
      mutate(
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
            male == 1 & expose_sex_m_nocondom == 1 ~ 1,
            male == 1 & yr_last_m >= p10y ~ 1,
            male == 1 & year(expose_sex_m_av_date) >= p10y ~ 1,          # HTS Form
            male == 1 & year(expose_sex_m_av_nocondom_date) >= p10y ~ 1, # HTS Form
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
            male == 1 & mot == 0 & expose_sex_f_nocondom == 1 ~ 3,
            male == 1 &
               mot == 0 &
               yr_last_f >= p10y ~ 3,
            male == 1 &
               mot == 0 &
               year(expose_sex_f_av_date) >= p10y ~ 3,          # HTS Form
            male == 1 &
               mot == 0 &
               year(expose_sex_f_av_nocondom_date) >= p10y ~ 3, # HTS Form
            TRUE ~ mot
         ),

         # f->m
         mot        = case_when(
            female == 1 & expose_sex_m_nocondom == 1 ~ 4,
            female == 1 & yr_last_m >= p10y ~ 4,
            female == 1 & year(expose_sex_m_av_date) >= p10y ~ 4,          # HTS Form
            female == 1 & year(expose_sex_m_av_nocondom_date) >= p10y ~ 4, # HTS Form
            TRUE ~ mot
         ),

         # IVDU
         mot        = case_when(
            expose_drug_inject > 0 & str_left(perm_prov, 4) == "0722" ~ 5,
            TRUE ~ mot
         ),

         # vertical
         mot        = case_when(
            mot == 0 & motherisi1 == 1 ~ 6,
            TRUE ~ mot
         ),

         # m->m-f hx
         mot        = case_when(
            male == 1 &
               mot == 0 &
               num_m_partner > 0 &
               is.na(yr_last_m) ~ 11,
            male == 1 &
               mot == 0 &
               yr_last_m >= p10y ~ 11,
            male == 1 &
               mot == 0 &
               !is.na(expose_sex_m_av_date) ~ 11,                     # HTS Form
            male == 1 &
               mot == 0 &
               !is.na(expose_sex_m_av_nocondom_date) ~ 11,            # HTS Form
            male == 1 & mot == 0 & expose_sex_m > 0 ~ 11,             # HTS Form
            TRUE ~ mot
         ),

         # m->m+f hx
         mot        = case_when(
            mot == 1 & num_f_partner > 0 & is.na(yr_last_f) ~ 21,
            mot == 3 & num_m_partner > 0 & is.na(yr_last_m) ~ 21,
            mot == 11 & num_f_partner > 0 & is.na(yr_last_f) ~ 21,
            mot == 11 & yr_last_f >= p10y ~ 21,
            mot == 11 & !is.na(expose_sex_f_av_date) ~ 21,          # HTS Form,
            mot == 11 & !is.na(expose_sex_f_av_nocondom_date) ~ 21, # HTS Form,
            mot == 11 & expose_sex_f > 0 ~ 21,                      # HTS Form,
            TRUE ~ mot
         ),

         # m->f hx
         mot        = case_when(
            male == 1 &
               mot == 0 &
               num_f_partner > 0 &
               is.na(yr_last_f) ~ 31,
            male == 1 &
               mot == 0 &
               yr_last_f >= p10y ~ 31,
            male == 1 &
               mot == 0 &
               !is.na(expose_sex_f_av_date) ~ 31,                     # HTS Form,
            male == 1 &
               mot == 0 &
               !is.na(expose_sex_f_av_nocondom_date) ~ 31,            # HTS Form,
            male == 1 & mot == 0 & expose_sex_f > 0 ~ 31,             # HTS Form,
            TRUE ~ mot
         ),

         # f->m hx
         mot        = case_when(
            female == 1 &
               mot == 0 &
               num_m_partner > 0 &
               is.na(yr_last_m) ~ 41,
            female == 1 &
               mot == 0 &
               yr_last_m >= p10y ~ 41,
            female == 1 &
               mot == 0 &
               !is.na(expose_sex_m_av_date) ~ 41,              # HTS Form,
            female == 1 &
               mot == 0 &
               !is.na(expose_sex_m_av_nocondom_date) ~ 41,     # HTS Form,
            female == 1 & mot == 0 & expose_sex_m > 0 ~ 41,    # HTS Form,
            TRUE ~ mot
         ),

         # IVDU hx
         mot        = case_when(
            injectdrug > 0 & str_left(perm_prov, 4) == "0722" ~ 51,
            TRUE ~ mot
         ),

         # mtct
         mot        = case_when(
            mot == 0 & age < 5 ~ 61,
            TRUE ~ mot
         ),

         # all else fails
         mot        = case_when(
            male == 1 & mot == 0 & num_m_partner > 0 ~ 1,
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

         # no data
         mot        = case_when(
            mot == 0 ~ 9,
            TRUE ~ mot
         ),

         # f->f
         mot        = case_when(
            female == 1 & mot == 0 & num_f_partner > 0 ~ 10,
            female == 1 & mot == 0 & !is.na(yr_last_f) > 0 ~ 10,
            female == 1 &
               mot == 0 &
               !is.na(expose_sex_f_av_date) ~ 10,
            female == 1 &
               mot == 0 &
               !is.na(expose_sex_f_av_nocondom_date) ~ 10,
            female == 1 & mot == 0 & expose_sex_f > 0 ~ 10,
            TRUE ~ mot
         ),

         # # clean mot_09
         # mot                  = case_when(
         #    mot %in% c(7, 8) ~ 9,
         #    TRUE ~ mot
         # ),

         # final filtering of mot using risk_*
         mot        = case_when(
            mot %in% c(7, 8, 9, 10) &
               male == 1 &
               str_detect(risk_sexwithm, "^yes") &
               str_detect(risk_sexwithf, "^yes") ~ 22,
            mot %in% c(7, 8, 9, 10) &
               male == 1 &
               str_detect(risk_sexwithm, "^yes") &
               !str_detect(risk_sexwithf, "^yes") ~ 12,
            mot %in% c(7, 8, 9, 10) &
               male == 1 &
               !str_detect(risk_sexwithm, "^yes") &
               str_detect(risk_sexwithf, "^yes") ~ 32,
            mot %in% c(7, 8, 9, 10) &
               female == 1 &
               str_detect(risk_sexwithm, "^yes") &
               !str_detect(risk_sexwithf, "^yes") ~ 42,
            mot %in% c(7, 8, 9, 10) & str_detect(risk_injectdrug, "^yes") ~ 52,
            TRUE ~ mot
         ),

         # transmit
         transmit   = case_when(
            mot %in% c(1, 2, 3, 4, 11, 12, 21, 22, 31, 32, 41, 42) ~ "sex",
            mot %in% c(5, 51, 52) ~ "IVDU",
            mot %in% c(6, 61) ~ "perinatal",
            mot %in% c(8, 9, 10) ~ "unknown",
            mot == 7 ~ "others",
         ),

         # sexhow
         sexhow     = case_when(
            mot %in% c(1, 11, 12) ~ "homosexual",
            mot %in% c(2, 21, 22) ~ "bisexual",
            mot %in% c(3, 4, 31, 32, 41, 42) ~ "heterosexual",
         ),
      ) %>%
      mutate(
         # cd4 tagging
         days_cd4_confirm     = interval(cd4_date, confirm_date) / days(1),
         cd4_is_baseline      = if_else(abs(days_cd4_confirm) <= 182, 1, 0, 0),

         # cd4_result           = NA_character_,
         # cd4_date             = NA_Date_,
         cd4_date             = case_when(
            cd4_is_baseline == 0 ~ NA_Date_,
            is.na(cd4_result) ~ NA_Date_,
            TRUE ~ cd4_date
         ),
         cd4_result           = case_when(
            cd4_is_baseline == 0 ~ NA_character_,
            TRUE ~ cd4_result
         ),
         cd4_result           = parse_number(cd4_result),
         baseline_cd4         = case_when(
            cd4_result >= 500 ~ 1,
            cd4_result >= 350 & cd4_result < 500 ~ 2,
            cd4_result >= 200 & cd4_result < 350 ~ 3,
            cd4_result >= 50 & cd4_result < 200 ~ 4,
            cd4_result < 50 ~ 5,
         ),

         # who Case Definition of advanced HIV classification
         # refined ahd
         ahd                  = case_when(
            who_staging %in% c(3, 4) ~ 1,
            age >= 5 & baseline_cd4 %in% c(4, 5) ~ 1,
            age < 5 ~ 1,
            !is.na(baseline_cd4) ~ 0
         ),
         baseline_cd4         = labelled(
            baseline_cd4,
            c(
               "1_500+ cells/μL"    = 1,
               "2_350-499 cells/μL" = 2,
               "3_200-349 cells/μL" = 3,
               "4_50-199 cells/μL"  = 4,
               "5_below 50"         = 5
            )
         ),

         # tb patient
         # class
         classd               = if_else(
            condition = !is.na(who_staging),
            true      = who_staging,
            false     = NA_integer_
         ) %>% as.numeric(),
         description_symptoms = stri_trans_toupper(symptoms),
         med_tb_px            = case_when(
            stri_detect_fixed(description_symptoms, "TB") ~ 1,
            TRUE ~ as.numeric(med_tb_px)
         ),
         classd               = case_when(
            stri_detect_regex(description_symptoms, paste(collapse = "|", (corr_classd %>% filter(as.numeric(class) == 3))$symptom)) ~ 3,
            med_tb_px == 1 ~ 3,
            TRUE ~ classd
         ),
         classd               = case_when(
            stri_detect_regex(description_symptoms, paste(collapse = "|", (corr_classd %>% filter(as.numeric(class) == 4))$symptom)) ~ 4,
            TRUE ~ classd
         ),

         # final class
         class                = case_when(
            classd %in% c(3, 4) ~ "aids",
            TRUE ~ "HIV"
         ),

         # new class for 2022
         class2022            = case_when(
            class == "aids" ~ "aids",
            ahd == 1 ~ "aids",
            TRUE ~ "HIV"
         ),

         # no data for stage of hiv
         nodata_hiv_stage     = if_else(
            if_all(c(who_staging, description_symptoms, med_tb_px, clinical_pic), ~is.na(.)),
            1,
            0,
            0
         ),

         # form (HTS)
         form_version         = if_else(form_version == " (vNA)", NA_character_, form_version),

         # provider type (HTS)
         provider_type        = as.integer(keep_code(provider_type)),

         # other services (HTS)
         given_ssnt           = case_when(
            service_ssnt_accept == 1 ~ "Accepted",
            service_ssnt_offer == 1 ~ "Offered",
         ),

         # combi prev (HTS)
         service_condoms      = if_else(service_condoms == 0, NA_integer_, as.integer(service_condoms), NA_integer_),
         service_lubes        = if_else(service_lubes == 0, NA_integer_, as.integer(service_lubes), NA_integer_),
      ) %>%
      arrange(central_id, desc(cd4_is_baseline), days_cd4_confirm, cd4_date) %>%
      distinct(central_id, .keep_all = TRUE) %>%
      ohasis$get_addr(
         c(
            region   = "perm_reg",
            province = "perm_prov",
            muncity  = "perm_munc"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            region_c   = "curr_reg",
            province_c = "curr_prov",
            muncity_c  = "curr_munc"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            region01   = "birth_reg",
            province01 = "birth_prov",
            placefbir  = "birth_munc"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            region_p   = "permonly_reg",
            province_p = "permonly_prov",
            muncity_p  = "permonly_munc"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            venue_region   = "hiv_service_reg",
            venue_province = "hiv_service_prov",
            venue_muncity  = "hiv_service_munc"
         ),
         "nhsss"
      ) %>%
      # country names
      left_join(
         y  = ohasis$ref_country %>%
            select(country_code, ocw_country = country_name),
         by = join_by(ofw_country == country_code)
      ) %>%
      relocate(ocw_country, .before = ofw_country) %>%
      # dxlab_standard
      mutate(
         use_specimen_source = is.na(test_faci) & !is.na(specimen_source),
         test_faci           = coalesce(if_else(use_specimen_source, specimen_source, test_faci, test_faci), ""),
         test_sub_faci       = coalesce(if_else(use_specimen_source, specimen_sub_source, test_sub_faci, test_sub_faci), ""),
      ) %>%
      left_join(
         na_matches = "never",
         y          = read_sheet("1aOqYjx5wbc403xy-64YHJU6NzhEBRUu6Ldg59yDEUMw", "Sheet1", range = "A:D", col_types = "c") %>%
            select(
               test_faci = harp_faci,
               pubpriv   = final_pubpriv
            ) %>%
            distinct(test_faci, .keep_all = TRUE) %>%
            mutate_all(~toupper(coalesce(., ""))),
         by         = join_by(test_faci)
      ) %>%
      mutate(
         form_faci_2        = test_faci,
         form_faci          = test_faci,
         sub_form_faci      = test_sub_faci,
         diff_source_v_form = if_else(coalesce(form_faci, "") != coalesce(specimen_source, "") & (sample_source == "R" | is.na(sample_source)), 1, 0, 0)
      ) %>%
      ohasis$get_faci(
         list(hts_faci = c("form_faci", "sub_form_faci")),
         "name"
      ) %>%
      ohasis$get_faci(
         list(source_faci = c("specimen_source", "specimen_sub_source")),
         "name"
      ) %>%
      # confirmlab
      ohasis$get_faci(
         list(confirmlab = c("confirm_faci", "confirm_sub_faci")),
         "code",
         c("confirm_region", "confirm_province", "confirm_muncity")
      ) %>%
      ohasis$get_faci(
         list(dxlab_standard = c("test_faci", "test_sub_faci")),
         "nhsss",
         c("dx_region", "dx_province", "dx_muncity")
      ) %>%
      rename(
         form_faci = form_faci_2
      ) %>%
      mutate(
         labcode2    = confirm_code,
         confirm_rec = rec_id,
      ) %>%
      # same vars as registry
      select(
         rec_id,
         central_id,
         patient_id,
         idnum,
         confirm_rec,
         hts_rec                   = hts_rec,
         form                      = form_version,
         modality                  = hts_modality,          # HTS Form
         consent_test              = test_agreed,           # HTS Form
         labcode                   = confirm_code,
         labcode2,
         year,
         month,
         uic                       = uic,
         firstname                 = first,
         middle                    = middle,
         last                      = last,
         name_suffix               = suffix,
         bdate                     = birthdate,
         patient_code              = patient_code,
         pxcode,
         age                       = age,
         age_months                = age_mo,
         sex                       = sex,
         philhealth                = philhealth_no,
         philsys_id                = philsys_id,
         mobile                    = client_mobile,
         email                     = client_email,
         muncity,
         province,
         region,
         muncity_c,
         province_c,
         region_c,
         muncity_p,
         province_p,
         region_p,
         ocw                       = is_ofw,
         motherisi1,
         pregnant                  = is_pregnant,
         tbpatient1                = med_tb_px,
         nationalit,
         civilstat                 = civil_status,
         self_identity,
         self_identity_other,
         gender_identity,
         nationality               = nationality,
         highest_educ              = educ_level,
         in_school                 = is_student,
         current_school_level,
         with_partner              = living_with_partner,
         child_count               = children,
         sexwithf,
         sexwithm,
         sexwithpro,
         regularlya,
         injectdrug,
         chemsex,
         receivedbt,
         sti,
         needlepri1,
         transmit,
         sexhow,
         mot,
         starts_with("risk_", ignore.case = FALSE),
         class,
         class2022,
         ahd,
         baseline_cd4,
         baseline_cd4_date         = cd4_date,
         baseline_cd4_result       = cd4_result,
         confirm_date,
         confirmlab,
         confirm_region,
         confirm_province,
         confirm_muncity,
         confirm_result            = confirm_result,
         confirm_remarks           = confirm_remarks,
         region01,
         province01,
         placefbir,
         curr_work,
         prev_work,
         ocw_based                 = ofw_station,
         ocw_country,
         age_sex                   = age_first_sex,
         age_inj                   = age_first_inject,
         howmanymse                = num_m_partner,
         yrlastmsex                = yr_last_m,
         howmanyfse                = num_f_partner,
         yrlastfsex                = yr_last_f,
         past12mo_injdrug          = expose_drug_inject,
         past12mo_rcvbt            = expose_blood_transfuse,
         past12mo_sti              = expose_sti,
         past12mo_sexfnocondom     = expose_sex_f_nocondom,
         past12mo_sexmnocondom     = expose_sex_m_nocondom,
         past12mo_sexprosti        = expose_sex_paying,
         past12mo_acceptpayforsex  = expose_sex_payment,
         past12mo_needle           = expose_occupation,
         past12mo_hadtattoo        = expose_tattoo,
         history_sex_m             = expose_sex_m,
         date_lastsex_m            = expose_sex_m_av_date,
         date_lastsex_condomless_m = expose_sex_m_av_nocondom_date,
         history_sex_f             = expose_sex_f,
         date_lastsex_f            = expose_sex_f_av_date,
         date_lastsex_condomless_f = expose_sex_f_av_nocondom_date,
         prevtest                  = prev_tested,
         prev_test_result          = prev_test_result,
         prev_test_faci            = prev_test_faci,
         prevtest_date             = prev_test_date,
         clinicalpicture           = clinical_pic,
         recombyph1                = test_reason_physician,
         recomby_peer_ed           = test_reason_peer_ed,   # HTS Form
         insurance1                = test_reason_insurance,
         recheckpr1                = test_reason_retest,
         no_test_reason            = test_reason_no_reason,
         possible_exposure         = test_reason_hiv_expose,
         emp_local                 = test_reason_employ_local,
         emp_abroad                = test_reason_employ_ofw,
         other_reason_test,
         description_symptoms,
         who_staging,
         hx_hepb                   = med_hep_b,
         hx_hepc                   = med_hep_c,
         hx_cbs                    = med_cbs_reactive,
         hx_prep                   = med_prep_px,
         hx_pep                    = med_pep_px,
         hx_sti                    = med_sti,
         reach_clinical            = reach_clinical,
         reach_online              = reach_online,
         reach_it                  = reach_index_testing,
         reach_ssnt                = reach_ssnt,
         reach_venue               = reach_venue,
         refer_art                 = refer_art,
         refer_confirm             = refer_confirm,
         retest                    = refer_retest,
         retest_in_mos             = retest_mos,
         retest_in_wks             = retest_wks,
         retest_date               = retest_date,
         given_hiv101              = service_hiv_101,
         given_iec_mats            = service_iec_mats,
         given_risk_reduce         = service_risk_counsel,
         given_prep_pep            = service_prep_refer,
         given_ssnt,
         provider_type             = provider_type,
         provider_type_other       = provider_type_other,
         venue_region,
         venue_province,
         venue_muncity,
         venue_text                = hiv_service_addr,
         px_type                   = client_type,
         referred_by               = refer_type,
         hts_date,
         t0_date                   = t0_date,
         t0_result                 = t0_result,
         test_done,
         name,
         t1_date                   = t1_date,
         t1_kit                    = t1_kit,
         t1_result                 = t1_result,
         t2_date                   = t2_date,
         t2_kit                    = t2_kit,
         t2_result                 = t2_result,
         t3_date                   = t3_date,
         t3_kit                    = t3_kit,
         t3_result                 = t3_result,
         final_interpretation      = confirm_result,
         visit_date,
         blood_extract_date,
         specimen_receipt_date,
         rhivda_done,
         sample_source,
         dxlab_standard,
         pubpriv,
         dx_region,
         dx_province,
         dx_muncity,
         diff_source_v_form,
         source_faci,
         hts_faci,
         # dup_munc,
         form_faci
      ) %>%
      # turn into codes
      mutate_at(
         .vars = vars(
            ocw,
            highest_educ,
            current_school_level,
            in_school,
            pregnant,
            with_partner,
            ocw_based,
            prev_test_result,
            clinicalpicture,
            prevtest,
            px_type,
            t1_result,
            t2_result,
            t3_result,
         ),
         ~as.integer(keep_code(.))
      ) %>%
      # remove codes
      mutate_at(
         .vars = vars(
            sex,
            civilstat,
            final_interpretation
         ),
         ~remove_code(.)
      ) %>%
      # fix test data
      mutate_at(
         .vars = vars(
            t1_result,
            t2_result,
            t3_result
         ),
         ~case_when(
            . == 1 ~ "Positive / Reactive",
            . == 2 ~ "Negative / Non-reactive",
            . == 3 ~ "Indeterminate",
            TRUE ~ NA_character_
         )
      ) %>%
      mutate(
         age_pregnant = if_else(
            condition = pregnant == 1,
            true      = age,
            false     = as.numeric(na)
         ),
         age_vertical = if_else(
            condition = transmit == "perinatal",
            true      = age,
            false     = as.numeric(na)
         ),
         age_unknown  = if_else(
            condition = transmit == "unknown",
            true      = age,
            false     = as.numeric(na)
         ),
         pubpriv      = if_else(pubpriv == "0", NA_character_, as.character(pubpriv))
      ) %>%
      distinct_all() %>%
      mutate(
         confirm_date     = coalesce(confirm_date, as.Date(t3_date)),
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

   return(converted)
}

changes_dx_v_hts <- function(rec_ids, yr, mo) {
   dx   <- stri_c("harp_dx.reg_", yr, stri_pad_left(mo, 2, "0"))
   con  <- connect("ohasis-lw")
   hts  <- QB$new(con)$
      from("ohasis_warehouse.form_hts as rec")$
      select("rec.*")$
      selectRaw("coalesce(id.central_id, rec.patient_id) as central_id")$
      leftJoin("ohasis_interim.registry as id", "rec.patient_id", "=", "id.patient_id")$
      whereIn("rec_id", rec_ids)$
      get()
   a    <- QB$new(con)$
      from("ohasis_warehouse.form_a as rec")$
      select("rec.*")$
      selectRaw("coalesce(id.central_id, rec.patient_id) as central_id")$
      leftJoin("ohasis_interim.registry as id", "rec.patient_id", "=", "id.patient_id")$
      whereIn("rec_id", rec_ids)$
      get()
   cfbs <- QB$new(con)$
      from("ohasis_warehouse.form_cfbs as rec")$
      select("rec.*")$
      selectRaw("coalesce(id.central_id, rec.patient_id) as central_id")$
      leftJoin("ohasis_interim.registry as id", "rec.patient_id", "=", "id.patient_id")$
      whereIn("rec_id", rec_ids)$
      get()
   cd4  <- QB$new(con)$
      from("ohasis_lake.lab_wide as cd4")$
      select("cd4.patient_id", "cd4.lab_cd4_date as cd4_date", "cd4.lab_cd4_result as cd4_result")$
      selectRaw("coalesce(id.central_id, cd4.patient_id) as central_id")$
      leftJoin("ohasis_interim.registry as id", "cd4.patient_id", "=", "id.patient_id")$
      whereNotNull("lab_cd4_date")$
      whereNotNull("lab_cd4_result")$
      get()
   dx   <- QB$new(con)$
      from(dx)$
      whereIn("rec_id", rec_ids)$
      get()
   dbDisconnect(con)

   records <- process_hts(hts, a, cfbs) %>%
      left_join(
         y  = cd4 %>%
            select(
               cd4_date,
               cd4_result,
               central_id
            ),
         by = join_by(central_id)
      ) %>%
      mutate(
         # calculate distance from confirmatory date
         cd4_date     = as.Date(cd4_date),
         cd4_confirm  = interval(cd4_date, date_confirm) / days(1),

         # baseline is within 182 days
         baseline_cd4 = if_else(
            cd4_confirm >= -182 & cd4_confirm <= 182,
            1,
            0
         ),

         # make values absolute to take date nearest to confirmatory
         cd4_confirm  = abs(cd4_confirm),
      ) %>%
      arrange(rec_id, cd4_confirm) %>%
      distinct(rec_id, .keep_all = TRUE) %>%
      arrange(desc(confirm_type), confirm_code)

   convert <- convert_dx(records)

   variables <- dx %>%
      summary.default %>%
      as.data.frame %>%
      group_by(Var1) %>%
      spread(key = Var2, value = Freq) %>%
      ungroup %>%
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
      filter(!(variable %in% c('final_interpretation', 'confirm_result', 'confirm_remarks')))

   return(check)
}
