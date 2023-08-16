get_baseline_cd4 <- function(cid, ref_date, lab_cd4, request = "date") {
   if (!is.Date(ref_date))
      ref_date <- as.Date(ref_date)

   min <- ref_date %m-% days(182)
   max <- ref_date %m+% days(182)

   ref_data <- lab_cd4 %>%
      filter(
         CENTRAL_ID == cid,
         CD4_DATE %within% interval(min, max)
      ) %>%
      mutate(
         CD4_DISTANCE = interval(CD4_DATE, ref_date) / days(1),
         CD4_DISTANCE = abs(CD4_DISTANCE)
      ) %>%
      arrange(CD4_DISTANCE, desc(CD4_DATE)) %>%
      slice(1)

   if (nrow(ref_data) > 0) {
      requested <- switch(
         request,
         date   = ref_data$CD4_DATE,
         result = ref_data$CD4_RESULT,
      )
   } else {
      requested <- switch(
         request,
         date   = NA_Date_,
         result = NA_character_,
      )
   }

   return(requested)
}