p_load(httr2, jsonlite)

get <- function(uri, token, ...) {
   req <- request(uri)

   if (!missing(token)) {
      req <- req_auth_bearer_token(req, token)
   }

   res  <- req_perform(req)
   type <- resp_content_type(res)
   data <- switch(
      type,
      `text/html`        = resp_body_string(res),
      `application/json` = resp_body_json(res),
   )

   return(data)
}

post <- function(uri, token, ...) {
   req <- request(uri)
   req <- req_method(req, "POST")

   if (!missing(token)) {
      req <- req_auth_bearer_token(req, token)
   }

   if (!missing(...)) {
      req <- req_body_form(req, ...)
   }

   res  <- req_perform(req)
   type <- resp_content_type(res)
   data <- switch(
      type,
      `text/html`        = resp_body_string(res),
      `application/json` = resp_body_json(res),
   )

   return(data)
}


con  <- connect("ohasis-lw")
faci <- QB$new(con)$from("ohasis_interim.facilities")$get()
sub  <- QB$new(con)$from("ohasis_interim.facility_unit")$get()
dbDisconnect(con)
uri <- "http://192.168.193.236:8000"

credentials <- list(
   USER_NAME = "jpalo.doh",
   PASSWORD  = "Jbrp1234"
)

token      <- post(file.path(uri, "api/login"), USER_NAME = "jpalo.doh", PASSWORD = "Jbrp1234")
facilities <- post(file.path(uri, "api/dropdowxn/facilities"), token)
supplies   <- post(file.path(uri, "api/supplies"), token) %>% bind_rows()
clients    <- post(file.path(uri, "api/client/list"), token, query = "JUAN dela cruz") %>% rbindlist(fill = TRUE)


facilities <- post(file.path(uri, "api/facility"), token) %>% rbindlist(fill = TRUE)

data <- file.path(uri, "api/dropdown/facilities") %>%
   request() %>%
   req_method("POST") %>%
   req_auth_bearer_token(token) %>%
   req_perform() %>%
   resp_body_json() %>%
   bind_rows()
data <- file.path(uri, "api/supplies") %>%
   request() %>%
   req_method("POST") %>%
   req_headers("Accept" = "application/json") %>%
   req_auth_bearer_token(token) %>%
   req_body_form(
      faci_id = "130001",
      date    = "2024-01-01"
   ) %>%
   req_perform() %>%
   resp_body_json() %>%
   bind_rows()


request(file.path(uri, "api/logout")) %>%
   req_auth_bearer_token(login) %>%
   req_body_form(token = login) %>%
   req_perform() %>%
   resp_body_html()