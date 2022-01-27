##------------------------------------------------------------------------------
##  Classes
##------------------------------------------------------------------------------

# main project class
Project <- setRefClass(
   Class   = "Project",
   fields  = list(
      mo        = "character",
      yr        = "character",
      run_title = "character"
   ),
   methods = list(
      initialize = function() {
         "This method is called when you create an instance of this class."

         # reporting date
         mo <<- .self$input(prompt = "What is the reporting month?", max.char = 2)
         mo <<- mo %>% stri_pad_left(width = 2, pad = "0")
         yr <<- .self$input(prompt = "What is the reporting year?", max.char = 4)
         yr <<- yr %>% stri_pad_left(width = 4, pad = "0")

         # label the current run
         run_title <<- .self$input(prompt = "Label the current run (brief, concise)")

         print("Project initialized!")
      },

      input      = function(prompt, default = NULL, max.char = NULL) {
         if (!is.null(default)) prompt <- paste0(prompt, " [", default, "]")

         # get user input
         data <- readline(paste0(prompt, ": "))

         # if empty, use default
         if (data == "" & !is.null(default))
            data <- default

         # if no default, throw error
         if (data == "" & is.null(default))
            stop("This is a required input!")

         # check if max characters defined
         if (!is.null(max.char) && nchar(data) > max.char)
            stop("Input exceeds the maximum number of characters!")


         # return value
         return(data)
      }
   )
)

# ohasis class
Ohasis <- setRefClass(
   Class   = "OHASIS",
   fields  = list(
      conn = "MariaDBConnection",
      db   = "list",
      ref  = "list"
   ),
   methods = list(
      initialize = function() {
         "This method is called when you create an instance of this class."

         # database connection
         conn <<- .self$check_conn()

         print("OHASIS initialized!")
      },

      # refreshes connection
      check_conn = function() {
         if (!dbIsValid(.self$conn))
            .self$conn <<- dbConnect(
               RMariaDB::MariaDB(),
               user     = Sys.getenv("DB_USER"),
               password = Sys.getenv("DB_PASS"),
               host     = Sys.getenv("DB_HOST"),
               port     = Sys.getenv("DB_PORT"),
               timeout  = -1,
               Sys.getenv("DB_NAME")
            )
      }
   )
)