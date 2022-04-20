##  Project Class --------------------------------------------------------------

# main project class
Project <- setRefClass(
   Class   = "Project",
   fields  = list(
      mo        = "character",
      yr        = "character",
      ym        = "character",
      date      = "Date",
      prev_mo   = "character",
      prev_yr   = "character",
      prev_date = "Date",
      next_mo   = "character",
      next_yr   = "character",
      next_date = "Date",
      run_title = "character"
   ),
   methods = list(
      # set the current reporting period
      set_report   = function() {
         # reporting date
         mo   <<- input(prompt = "What is the reporting month?", max.char = 2)
         mo   <<- mo %>% stri_pad_left(width = 2, pad = "0")
         yr   <<- input(prompt = "What is the reporting year?", max.char = 4)
         yr   <<- yr %>% stri_pad_left(width = 4, pad = "0")
         ym   <<- paste0(yr, ".", mo)
         date <<- as.Date(paste(sep = "-", yr, mo, "01"))

         # prev date
         dates     <- .self$get_date_ref("prev", yr, mo)
         prev_mo   <<- dates$mo
         prev_yr   <<- dates$yr
         prev_date <<- as.Date(paste(sep = "-", prev_yr, prev_mo, "01"))

         # next date
         dates     <- .self$get_date_ref("next", yr, mo)
         next_mo   <<- dates$mo
         next_yr   <<- dates$yr
         next_date <<- as.Date(paste(sep = "-", next_yr, next_mo, "01"))

         # label the current run
         run_title <<- input(prompt = "Label the current run (brief, concise)")

         .log_success("Project parameters defined!")
      },

      # get reference dates for the report
      get_date_ref = function(type = NULL, yr = NULL, mo = NULL) {
         # next dates
         if (type == 'next') {
            dateMo <- ifelse(as.numeric(mo) == 12, '01', stringi::stri_pad_left(as.character(as.numeric(mo) + 1), 2, '0'))
            dateYr <- ifelse(as.numeric(mo) == 12, as.numeric(yr) + 1, as.numeric(yr)) %>% as.character()
         }

         # prev dates
         if (type == 'prev') {
            dateMo <- ifelse(as.numeric(mo) == 1, '12', stringi::stri_pad_left(as.character(as.numeric(mo) - 1), 2, '0'))
            dateYr <- ifelse(as.numeric(mo) == 1, as.numeric(yr) - 1, as.numeric(yr)) %>% as.character()
         }

         dateReturn <- list(mo = dateMo, yr = dateYr)

         return(dateReturn)
      },

      # get official dataset files
      get_data     = function(surveillance = NULL, refYr = NULL, refMo = NULL, path = NULL, file_type = "dta") {

         metadata <- function() {
            if (tolower(surveillance) == "harp_dx") {
               path    <- Sys.getenv("HARP_DX")
               pattern <- paste0('*reg_', refYr, '-', refMo, '.*\\.', file_type)
            }

            if (tolower(surveillance) == "harp_dead") {
               path    <- Sys.getenv("HARP_DEAD")
               pattern <- paste0('*mort_', refYr, '-', refMo, '.*\\.', file_type)
            }

            if (tolower(surveillance) == "harp_tx-reg") {
               path    <- Sys.getenv("HARP_TX")
               pattern <- paste0('*reg-art_', refYr, '-', refMo, '.*\\.', file_type)
            }

            if (tolower(surveillance) == "harp_tx-outcome") {
               path    <- Sys.getenv("HARP_TX")
               pattern <- paste0('*onart_', refYr, '-', refMo, '.*\\.', file_type)
            }

            return(c(path, pattern))
         }

         # function to find the latest file
         get_latest <- function(path, pattern)
            sort(list.files(path = path, full.names = TRUE, pattern = pattern), decreasing = TRUE)

         # initiate the first file
         info <- metadata()
         file <- get_latest(info[1], info[2])

         # if first file called is non-existent, look to previous month's report
         while (length(file) == 0) {
            if (as.numeric(refMo) > 1) {
               # for months feb-dec, check previous month
               refMo <- as.numeric(refMo) - 1
            } else {
               # for jan, check previous year & month of dec
               refMo <- 12
               refYr <- as.numeric(refYr) - 1
            }
            refMo <- str_pad(refMo, 2, 'left', '0')
            refYr <- as.character(refYr)

            info <- metadata()
            file <- get_latest(info[1], info[2])
         }

         return(file[1])
      }
   )
)