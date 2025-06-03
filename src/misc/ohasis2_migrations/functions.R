OhasisMigration <- R6Class(
   'OhasisMigration',
   public  = list(
      table      = NA_character_,
      data       = list(
         from = tibble(),
         to   = tibble()
      ),

      initialize = function(table, conn_from, conn_to) {
         self$table <- table

         private$conn$from <- conn_from
         private$conn$to   <- conn_to
      },
      download   = function() {
         conn_from <- connect(private$conn$from)
         conn_to   <- connect(private$conn$to)

         self$data$from <- QB$new(conn_from)$from(self$table)$get()
         self$data$to   <- QB$new(conn_to)$from(self$table)$get()

         dbDisconnect(conn_from)
         dbDisconnect(conn_to)

         invisible(self)
      },
      disconnect = function() {
         dbDisconnect(private$conn$from)
         dbDisconnect(private$conn$to)

         invisible(self)
      },
      upload     = function(data, id_col) {
         conn_to <- connect(private$conn$to)
         dbAppendTable(conn_to, self$table, data, id_col, batch_size = 500)
         dbDisconnect(conn_to)
      }
   ),
   private = list(
      conn = list(
         from = NULL,
         to   = NULL
      )
   )
)