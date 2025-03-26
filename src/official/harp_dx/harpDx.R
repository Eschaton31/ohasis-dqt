harpDx <- R6Class(
   "harpDx",
   public  = list(
      yr          = any(),
      mo          = any(),
      ym          = NA_character_,
      min         = NA_Date_,
      max         = NA_Date_,

      initialize  = function(end_date) {
         if (missing(end_date)) {
            end_date <- Sys.Date()
         }

         if (is.character(end_date)) {
            end_date <- as.Date(end_date)
         }

         self$max <- end_date
         self$min <- floor_date(self$max, "months")
         self$yr  <- year(self$max)
         self$mo  <- month(self$max)
         self$ym  <- format(self$max, "%Y.%m")
      },

      updateForms = function(from = NULL, to = NULL) {
         lapply(private$tables$lake, ohasis$data_factory, db_type = "lake", update_type = "upsert", default_yes = TRUE, from = from, to = to)
         lapply(private$tables$warehouse, ohasis$data_factory, db_type = "warehouse", update_type = "upsert", default_yes = TRUE, from = from, to = to)
      }
   ),
   private = list(
      tables = list(
         lake      = c(
            "px_pii",
            "px_faci_info",
            "px_ob",
            "px_hiv_testing",
            "px_consent",
            "px_occupation",
            "px_ofw",
            "px_risk",
            "px_expose_profile",
            "px_test_reason",
            "px_test_refuse",
            "px_test_previous",
            "px_med_profile",
            "px_staging",
            "px_cfbs",
            "px_reach",
            "px_linkage",
            "px_other_service"
         ),
         warehouse = c(
            "form_a",
            "form_hts",
            "id_registry"
         )
      )
   )
)

try <- harpDx$new("2024-07-31")
try$updateForms()