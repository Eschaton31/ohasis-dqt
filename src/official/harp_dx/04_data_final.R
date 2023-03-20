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
   for (drop_var in c("drop_notyet", "drop_duplicates"))
      if (drop_var %in% names(nhsss$harp_dx$corr))
         nhsss$harp_dx$official$new %<>%
            left_join(
               y  = nhsss$harp_dx$corr[[drop_var]] %>%
                  select(REC_ID) %>%
                  mutate(drop = 1),
               by = "REC_ID"
            ) %>%
            mutate(
               drop_var := if_else(
                  condition = drop == 1,
                  true      = 1,
                  false     = !!as.symbol(drop_var),
                  missing   = !!as.symbol(drop_var)
               )
            ) %>%
            select(-drop)
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