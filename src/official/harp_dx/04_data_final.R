##  Append w/ old Registry -----------------------------------------------------

append_data <- function(old, new) {
   data <- old %>%
      mutate(consent_test = as.integer(consent_test)) %>%
      bind_rows(new) %>%
      arrange(idnum) %>%
      mutate(
         drop_notyet     = 0,
         drop_duplicates = 0,
      )

   return(data)
}

##  Tag data to be reported later on and duplicates for dropping ---------------

tag_fordrop <- function() {
   for (drop_var in c("drop_notyet", "anti_join"))
      if (drop_var %in% names(nhsss$harp_dx$corr) && nrow(nhsss$harp_dx$corr[[drop_var]]) > 0) {
         drop_var <- ifelse(drop_var == "anti_join", as.name("drop_duplicates"), as.name(drop_var))
         nhsss$harp_dx$official$new %<>%
            left_join(
               y  = nhsss$harp_dx$corr[[drop_var]] %>%
                  select(REC_ID) %>%
                  mutate(drop = 1),
               by = "REC_ID"
            ) %>%
            mutate(
               !!drop_var := if_else(
                  condition = drop == 1,
                  true      = 1,
                  false     = !!drop_var,
                  missing   = !!drop_var
               )
            ) %>%
            select(-drop)
      }
}

##  Subsets for documentation --------------------------------------------------

subset_drops <- function() {
   local(envir = nhsss$harp_dx, {
      official$dropped_notyet     <- official$new %>%
         filter(drop_notyet == 1)
      official$dropped_duplicates <- official$new %>%
         filter(drop_duplicates == 1)
   })
}

##  Drop using taggings --------------------------------------------------------

generate_final <- function(data) {
   data %<>%
      mutate(
         labcode2    = if_else(
            condition = is.na(labcode2),
            true      = labcode,
            false     = labcode2,
            missing   = labcode2
         ),
         drop        = drop_duplicates + drop_notyet,
         who_staging = as.integer(who_staging)
      ) %>%
      filter(drop == 0) %>%
      select(-drop, -drop_duplicates, -drop_notyet, -mot)

   final_new <- data %>%
      filter(year == as.numeric(ohasis$yr), month == as.numeric(ohasis$mo))

   nrow_new  <- nrow(final_new)
   nrow_ahd  <- final_new %>%
      filter(class2022 == "AIDS") %>%
      nrow()
   perc_ahd  <- stri_c(format((nrow_ahd / nrow_new) * 100, digits = 2), "%")
   nrow_none <- final_new %>%
      filter(transmit == "UNKNOWN") %>%
      nrow()
   perc_none <- stri_c(format((nrow_none / nrow_new) * 100, digits = 2), "%")
   nrow_mtct <- final_new %>%
      filter(transmit == "PERINATAL") %>%
      nrow()
   perc_mtct <- stri_c(format((nrow_mtct / nrow_new) * 100, digits = 2), "%")

   log_info("New cases    = {green(stri_pad_left(nrow_new, 4, ' '))}.")
   log_info("New AHD      = {green(stri_pad_left(nrow_ahd, 4, ' '))}, {red(perc_ahd)}.")
   log_info("New Unknown  = {green(stri_pad_left(nrow_none, 4, ' '))}, {red(perc_none)}.")
   log_info("New Vertical = {green(stri_pad_left(nrow_mtct, 4, ' '))}, {red(perc_mtct)}.")
   return(data)
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      old <- .GlobalEnv$nhsss$harp_dx$official$old
      new <- read_rds(file.path(wd, "converted.RDS"))

      .GlobalEnv$nhsss$harp_dx$official$new <- append_data(old, new)
      rm(new, old)

      tag_fordrop()
      subset_drops()
      .GlobalEnv$nhsss$harp_dx$official$new %<>% generate_final()

      write_rds(.GlobalEnv$nhsss$harp_dx$official$new, file.path(wd, "final.RDS"))
   })
}