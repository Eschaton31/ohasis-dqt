## if not installed, use this:
# install.packages('dplyr')

library(dplyr)
library(stringr)
library(stringi)
library(haven)
library(readxl)
library(writexl)

file   <- "C:/Users/johnb/Downloads/20240513103735-Form A (v2017).xlsx"
sheets <- excel_sheets(file)
xl     <- read_excel(file, sheets[2])

xl           <- list()
xl[['shet']] <- read_excel(file, "Sheet1")
for (s in sheets) {
   xl[[s]] <- read_excel(file, s)
}

tab(xl$Sheet1, Sex)


con <- ohasis$conn("lw")
art <- QB$new(con)$from("harp_tx.outcome_202403")$get()
dbDisconnect(con)

analysis <- art %>%
   select(
      art_id,
      outcome,
      latest_ffupdate,
      latest_nextpickup
   ) %>%
   mutate(
      .after    = outcome,
      outcome28 = hiv_tx_outcome(outcome, latest_nextpickup, as.Date("2024-03-31"), 3, "months")
   )

analysis %>%
   tab(outcome, outcome28)
