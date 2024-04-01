#' @apiTitle Plumber API to encrypt cookies
#' @apiDescription A filter and endpoints for using encrypted cookies

protected <- c(
   "patient",
   "inventory"
)

# This is the key for encryption
keyring::key_set_with_value("plumber_api", password = plumber::random_cookie_key())
key <- keyring::key_get("plumber_api")

# Programmatically alter your API
#' @plumber
function(pr) {
   pr %>%
      # Overwrite the default serializer to return unboxed JSON
      pr_set_serializer(serializer_unboxed_json()) %>%
      # Add the encrypted cookie function, cookie called "token", encrypted with key valye
      pr_cookie(key, "token")
}


#' Check that a token is provided and is valid for carrying out request. This
#' filter is run for every request unless a `@preempt token_check` is used.
#'
#' @param req,res plumber request and response objects
token_check <- function(req, res) {

   if (str_detect(stri_c("/", res$PATH_INFO), protected)) {

      if (is.null(req$session$token)) {
         res$status <- 401
         return(list(error = "Token not found."))
      }

      if (Sys.time() > req$session$token) {
         res$status <- 401
         return(list(error = "Token expired."))
      }

   }

   plumber::forward()

}

#' Refresh user token
#'
#' Return token in HTTP header "token". This function excludes the `token_check`
#' filter.
#'
#' @param req,res plumber request and response objects
#'
#' Expects a request body with `user` and `password`
#'
#' @post /refresh-token
function(req, res) {

   any_missing_credentials <- is.null(req$body$user) && is.null(req$body$password)

   if (any_missing_credentials) {
      res$status <- 400
      return(list(error = "Credentials not found in request body."))
   }

   req_user <- req$body$user

   conn     <- dbConnect(MariaDB(), group = "ohasis-live", default.file = "my.cnf")
   user_row <- dbGetQuery(conn, "SELECT USER_ID, USER_NAME, PASSWORD FROM users WHERE USER_NAME = ?", params = list(req_user))
   dbDisconnect(conn)

   if (nrow(user_row) == 0) {
      res$status <- 401
      return(list(error = "Invalid credentials."))
   }

   if (req$body$password != user_row$PASSWORD) {
      res$status <- 401
      return(list(error = "Invalid credentials."))
   }

   req$session$token <- Sys.time() + (60 * 10)

   return(list(token_expiry = req$session$token))

}
