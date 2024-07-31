summarise_art <- function(data) {
   sum <- data %>%
      faci_code_to_id(
         ohasis$ref_faci_code,
         list(FACI_ID = "realhub", SUB_FACI_ID = "realhub_branch")
      ) %>%
      ohasis$get_faci(
         list(site = c("FACI_ID", "SUB_FACI_ID")),
         "name",
         c("reg", "prov", "munc")
      ) %>%
      group_by(reg, prov, munc, site) %>%
      summarise(
         `Alive on ARV`      = sum(outcome == 'alive on arv'),
         `Lost to follow-up` = sum(outcome == 'lost to follow up'),
         `Stopped ART`       = sum(str_detect(outcome, 'stop')),
         `Expired`           = sum(outcome == 'dead'),
      ) %>%
      ungroup() %>%
      mutate(
         `Retention Coverage` = `Alive on ARV` / (`Alive on ARV` + `Lost to follow-up`)
      )

   return(sum)
}

art <- list(
   prev = read_dta(hs_data("harp_tx", "outcome", 2024, 4)),
   curr = read_dta(hs_data("harp_tx", "outcome", 2024, 5))
)

art <- lapply(art, summarise_art)

all <- art$prev %>%
   full_join(art$curr, join_by(reg, prov, munc, site), suffix = c(" (Apr 2024)", " (May 2024)")) %>%
   rename(
      Region       = reg,
      Province     = prov,
      Municipality = munc
   )

dir <- "H:/Regional ART 202405v202404"
by_reg <- split(all, ~Region)
for (reg in names(by_reg)) {
   write_flat_file(list(Summary = by_reg[[reg]]), file.path(dir, stri_c(reg, ".xlsx")))
}