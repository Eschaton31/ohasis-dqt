HarpVl <- R6Class(
   'HarpVl',
   public = list(
      params     = list(
         yr  = NA_character_,
         mo  = NA_character_,
         end = NA_character_
      ),
      data       = list(
         forms     = tibble(),
         id_reg    = tibble(),
         converted = tibble()
      ),

      initialize = function(yr, mo) {
         self$params$yr  <- stri_pad_left(yr, 4, '0')
         self$params$mo  <- stri_pad_left(mo, 2, '0')
         self$params$end <- as.character(end_ym(yr, mo))

         invisible(self)
      },

      fetch      = function() {
         lw_conn          <- connect('mariadb-lw')
         self$data$id_reg <- update_idreg()
         self$data$forms  <- QB$new(lw_conn)$
            select("form.rec_id",
                   "pii.patient_id",
                   "pii.faci_id",
                   "pii.sub_faci_id",
                   "provider.service_faci",
                   "provider.service_sub_faci",
                   "form.lab_viral_date",
                   "form.lab_viral_result",
                   "pii.record_date")$
            from('ohasis_lake.lab_wide as form')$
            join('ohasis_lake.px_demographics as pii', 'form.rec_id', '=', 'pii.rec_id')$
            leftJoin('ohasis_lake.px_provider as provider', 'form.rec_id', '=', 'provider.rec_id')$
            whereNotNull("form.lab_viral_date")$
            whereNotNull("form.lab_viral_result")$
            whereNull("pii.deleted_at")$
            where('form.lab_viral_date', '<=', self$params$end)$
            get()
         dbDisconnect(lw_conn)

         invisible(self)
      },

      convert    = function() {
         self$data$converted <- self$data$forms %>%
            # get latest central ids
            get_cid(self$data$id_reg, patient_id) %>%
            select(
               central_id,
               faci_id,
               sub_faci_id,
               service_faci,
               service_sub_faci,
               visit_date        = record_date,
               vl_date           = lab_viral_date,
               vl_result_encoded = lab_viral_result,
            ) %>%
            process_vl("vl_result_encoded", "vl_result_clean") %>%
            rename_with(tolower) %>%
            mutate(res_tag = 2) %>%
            distinct() %>%
            select(-matches('hub')) %>%
            mutate(
               final_faci   = coalesce(service_faci, faci_id),
               final_sub    = coalesce(service_sub_faci, sub_faci_id),
               final_faci_2 = final_faci,
               final_sub_2  = final_sub,
            ) %>%
            ohasis$get_faci(
               list("facility_name" = c("final_faci", "final_sub")),
               "name"
            ) %>%
            ohasis$get_faci(
               list("hub" = c("final_faci_2", "final_sub_2")),
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
            # select(
            #    central_id,
            #    hub,
            #    facility_name,
            #    res_tag,
            #    vl_date,
            #    vl_result_encoded,
            #    vl_result_clean,
            #    vl_error,
            #    vl_drop
            # ) %>%
            mutate(
               vl_sort  = case_when(
                  if_all(c(vl_date, vl_result_clean), ~!is.na(.)) ~ 1,
                  !is.na(vl_date) & is.na(vl_result_clean) ~ 2,
                  is.na(vl_date) & !is.na(vl_result_clean) ~ 3,
                  TRUE ~ 9999
               ),
               vl_drop  = coalesce(if_else(vl_sort == 9999, 1, vl_drop, vl_drop), 0),
               vl_error = coalesce(vl_error, 0)
            )
      },

      export     = function() {
         output_version <- format(Sys.time(), "%Y%m%d")
         output_name    <- paste0(output_version, '_vldata_', self$params$yr, '-', self$params$mo)
         file_vl        <- file.path(Sys.getenv("HARP_VL"), paste0(output_name, ".dta"))

         log_info("Saving in Stata data format.")
         write_dta(
            data = self$data$converted,
            path = file_vl
         )
      }
   )
)

# try <- HarpVl$new(2026, 6)
# try$fetch()
# try$convert()
# try$export()
