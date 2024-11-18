upload_corr <- function(data, surv_name, id) {
   conn <- ohasis$conn("lw")
   dbxUpsert(
      conn,
      Id(schema = surv_name, table = "corr_reg"),
      data,
      c(id, "period", "variable")
   )
   dbDisconnect(conn)
}

restore_old_dx <- function(from, to, variable, format) {
   var    <- as.name(variable)
   period <- to

   from <- str_split_1(from, "\\.")
   to   <- str_split_1(to, "\\.")

   prev <- read_dta(hs_data("harp_dx", "reg", from[1], from[2]), col_select = c(idnum, {{var}})) %>%
      rename(new_value = !!var) %>%
      mutate_if(
         .predicate = is.labelled,
         ~to_character(.)
      )
   curr <- read_dta(hs_data("harp_dx", "reg", to[1], to[2]), col_select = c(idnum, {{var}})) %>%
      rename(old_value = !!var)
   corr <- prev %>%
      mutate_if(
         .predicate = is.character,
         ~na_if(., "")
      ) %>%
      filter(!is.na(new_value)) %>%
      left_join(curr, join_by(idnum)) %>%
      mutate(
         period   = period,
         variable = variable,
         format   = format
      ) %>%
      select(
         period,
         idnum,
         variable,
         old_value,
         new_value,
         format
      ) %>%
      mutate_at(vars(old_value, new_value), as.character) %>%
      filter(coalesce(old_value, "") != coalesce(new_value, ""))

   return(corr)
}

periods <- c("2024.06", "2024.07", "2024.08", "2024.09")
vars    <- c('baseline_cd4', 'clinicalpicture', 'consent_test', 'current_school_level', 'highest_educ', 'in_school', 'ocw_based', 'past12mo_acceptpayforsex', 'past12mo_hadtattoo', 'past12mo_injdrug', 'past12mo_needle', 'past12mo_rcvbt', 'past12mo_sexfnocondom', 'past12mo_sexmnocondom', 'past12mo_sexprosti', 'past12mo_sti', 'prevtest', 'prev_test_result', 'provider_type', 'px_type', 'who_staging', 'with_partner')
pblapply(periods, function(period) {
   pblapply(vars, function(var, period) {
      try <- restore_old_dx(period, "2024.10", var, "integer")
      upload_corr(try, "harp_dx", "idnum")
   }, period)
})

period <- "2022.12"
var    <- "ahd"