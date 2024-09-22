OhasisData <- R6Class(
   "OhasisData",
   public  = list(
      periods    = list(
         curr  = c(
            yr  = NULL,
            mo  = NULL,
            ym  = NULL,
            min = NA_Date_,
            max = NA_Date_
         ),
         prev  = c(
            yr  = NULL,
            mo  = NULL,
            ym  = NULL,
            min = NA_Date_,
            max = NA_Date_
         ),
         after = c(
            yr  = NULL,
            mo  = NULL,
            ym  = NULL,
            min = NA_Date_,
            max = NA_Date_
         )
      ),
      refs       = list(),

      initialize = function(end_date) {
         if (missing(end_date)) {
            end_date <- Sys.Date()
         }

         if (is.character(end_date)) {
            end_date <- as.Date(end_date)
         }

         self$periods$curr  <- private$calculatePeriod(end_date)
         self$periods$prev  <- private$calculatePeriod(end_date %m-% months(1))
         self$periods$after <- private$calculatePeriod(end_date %m+% months(1))
      },

      fetchRefs  = function() {
         log_info("Downloading references.")

         db_conn               <- connect("ohasis-live")
         self$refs$ref_country <- QB$new(db_conn)$from("ohasis_interim.addr_country")$get()
         dbDisconnect(db_conn)

         lw_conn             <- connect("ohasis-lw")
         self$refs$addr      <- QB$new(lw_conn)$from("ohasis_lake.ref_addr")$get()
         self$refs$staff     <- QB$new(lw_conn)$from("ohasis_lake.ref_staff")$get()
         self$refs$faci      <- QB$new(lw_conn)$from("ohasis_lake.ref_faci")$get()
         self$refs$faci_code <- self$refs$faci %>%
            filter(!is.na(FACI_CODE)) %>%
            mutate(
               branch_priority = case_when(
                  FACI_ID == "130001" ~ 1,
                  FACI_ID == "130605" ~ 2,
                  FACI_ID == "130748" ~ 3,
                  TRUE ~ 9999
               )
            ) %>%
            arrange(branch_priority) %>%
            distinct(FACI_CODE, .keep_all = TRUE) %>%
            rename(
               SUB_FACI_CODE = FACI_CODE
            ) %>%
            mutate(
               FACI_CODE     = case_when(
                  stri_detect_regex(SUB_FACI_CODE, "^HASH") ~ "HASH",
                  stri_detect_regex(SUB_FACI_CODE, "^SAIL") ~ "SAIL",
                  stri_detect_regex(SUB_FACI_CODE, "^TLY") ~ "TLY",
                  TRUE ~ SUB_FACI_CODE
               ),
               SUB_FACI_CODE = if_else(
                  condition = nchar(SUB_FACI_CODE) == 3,
                  true      = NA_character_,
                  false     = SUB_FACI_CODE
               ),
               SUB_FACI_CODE = case_when(
                  FACI_CODE == "HASH" & is.na(SUB_FACI_CODE) ~ "HASH-QC",
                  FACI_CODE == "TLY" & is.na(SUB_FACI_CODE) ~ "TLY-ANGLO",
                  FACI_CODE == "SHP" & is.na(SUB_FACI_CODE) ~ "SHIP-MAKATI",
                  TRUE ~ SUB_FACI_CODE
               ),
            ) %>%
            relocate(FACI_CODE, SUB_FACI_CODE, .before = 1)

         dbDisconnect(lw_conn)

         log_success("Done.")
         invisible(self)
      }
   ),
   private = list(
      calculatePeriod = function(end_date) {
         max <- end_date
         min <- floor_date(max, "months")
         yr  <- year(max)
         mo  <- month(max)
         ym  <- format(max, "%Y.%m")

         return(c(yr = yr, mo = mo, ym = ym, min = min, max = max))
      }
   )
)

harp <- OhasisData$new()
harp$fetchRefs()