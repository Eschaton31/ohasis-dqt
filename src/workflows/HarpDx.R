HarpDx <- R6Class(
   "HarpDx",
   inherit = OhasisData,
   public  = list(
      wd               = here("src", "official", "harp_dx"),
      data             = list(
         corr  = list(),
         forms = list(),
         pdf   = tibble()
      ),
      official         = list(
         old_reg = tibble(),
         new_reg = tibble(),
         dupes   = tibble()
      ),

      initialize       = function(end_date) {
         super$initialize(end_date)
         invisible(self)
      },

      updateNewCases   = function() {
         self$tableFromSql("dx_new", self$wd)
         invisible(self)
      },

      fetchForms       = function() {
         sql <- list(
            form_a       = read_file(file.path(self$wd, "form_a.sql")),
            form_hts     = read_file(file.path(self$wd, "form_hts.sql")),
            form_cfbs    = read_file(file.path(self$wd, "form_cfbs.sql")),
            px_confirmed = read_file(file.path(self$wd, "px_confirmed.sql")),
            cd4          = read_file(file.path(self$wd, "lab_cd4.sql"))
         )

         conn <- connect("ohasis-lw")
         data <- list()
         for (table in names(sql)) {
            if (table == "cd4")
               data[[table]] <- tracked_select(conn, sql[[table]], table, list(self$periods$curr$max))
            else
               data[[table]] <- tracked_select(conn, sql[[table]], table)
         }
         data$non_dupes <- QB$new(conn)$select(PATIENT_ID, NON_PAIR_ID)$from("ohasis_warehouse.non_dupes")$get()
         dbDisconnect(conn)

         self$data$forms <- data

         invisible(self)
      },

      fetchCorrections = function() {
         self$data$corr <- flow_corr(self$periods$curr$ym, "harp_dx")
         invisible(self)
      },

      fetchRegistry    = function() {
         ym      <- stri_replace_all_fixed(self$periods$prev$ym, ".", "")
         corr    <- self$data$corr$corr_reg
         drop    <- self$data$corr$corr_drop
         old_reg <- private$fetchPreviousDataset("harp_dx", "reg", ym, "idnum", corr, drop)

         self$official$old_reg <- old_reg
         self$official$dupes   <- get_dupes(old_reg, CENTRAL_ID)

         invisible(self)
      },

      checkRhivdaPdf   = function() {
         log_info("Loading list of rHIVda PDF Results.")
         path   <- file.path(Sys.getenv("DRIVE_DROPBOX"), "File requests", "rHIVda Submission", "FORMS", self$periods$curr$yr, self$periods$curr$ym)
         rhivda <- dir_info(path, recurse = TRUE)
         rhivda %<>%
            filter(type == "file") %>%
            mutate(
               .before      = 1,
               CONFIRM_CODE = str_extract(basename(path), "[A-Z][A-Z][A-Z][0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9][0-9]"),
            ) %>%
            filter(!is.na(CONFIRM_CODE))

         self$data$pdf <- rhivda

         invisible(self)
      }
   ),
   private = list(
      tables                 = list(
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
      ),

      generateNewlyConfirmed = function() {

      }
   )
)

try <- HarpDx$new("2024-10-31")
# try$updateForms()
# try$updateNewCases()
try$fetchForms()
try$fetchCorrections()
try$fetchRegistry()

hts          <- process_hts(try$data$forms$form_hts, try$data$forms$form_a, try$data$forms$form_cfbs)
confirm_cols <- names(forms$px_confirm)
confirm_cols <- confirm_cols[!(confirm_cols %in% c("REC_ID", "CENTRAL_ID"))]

data %<>%
   arrange(CONFIRM_RESULT, FORM_SORT, lab_year, lab_month, desc(CONFIRM_TYPE), hts_date, confirm_date) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   filter(report_date < ohasis$next_date | is.na(report_date)) %>%
   rename(
      TEST_FACI     = SERVICE_FACI,
      TEST_SUB_FACI = SERVICE_SUB_FACI,
   )