#' Get Central ID of Patient
#'
#' Returns the duplicates information for a `PATIENT_ID` from the OHASIS registry.
#'
#' @param patient_id `PATIENT_ID`
#'
#' @get /duplicates/<patient_id>
function(patient_id) {

   if (is.null(patient_id)) {
      return(list(error = "Missing `patient_id`."))
   }

   conn <- dbConnect(MariaDB(), group = "ohasis-live", default.file = "C:/nhsss/projects/ohasis-dqt/my.cnf")

   data_cid   <- dbGetQuery(conn, "SELECT CENTRAL_ID FROM registry WHERE PATIENT_ID = ?", params = list(patient_id))
   central_id <- ifelse(nrow(data_cid) > 0, data_cid$CENTRAL_ID, patient_id)

   data_pid    <- dbGetQuery(conn, "SELECT PATIENT_ID FROM registry WHERE CENTRAL_ID = ?", params = list(central_id))
   patient_ids <- data_pid$PATIENT_ID

   dbDisconnect(conn)


   return(list(central_id = central_id, duplicates = patient_ids))

}
