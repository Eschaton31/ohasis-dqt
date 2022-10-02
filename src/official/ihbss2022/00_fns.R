ihbss_mos <- function(data, col, varname) {
   data %>%
      select(
         id,
         months = col
      ) %>%
      separate(
         months,
         sep  = " ",
         into = c("m01", "m02", "m03", "m04", "m05", "m06", "m07", "m08", "m09", "m10", "m11", "m12")
      ) %>%
      pivot_longer(
         cols      = starts_with("m"),
         names_to  = "num",
         values_to = "month"
      ) %>%
      filter(!is.na(month)) %>%
      arrange(month) %>%
      mutate(
         month = month.abb[as.numeric(month)],
         num   = 1,
      ) %>%
      pivot_wider(
         id_cols      = id,
         names_from   = month,
         values_from  = num,
         names_prefix = paste0(varname, "_")
      )
}