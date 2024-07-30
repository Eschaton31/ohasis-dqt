##  EB LaBBS U -----------------------------------------------------------------
labbs <- read_excel("C:/Users/johnb/Downloads/Q1-Q4_2023_LaBBS Consolidation.v3.xlsx", "1. Facilities")

awards_labbs <- labbs %>%
   select(
      Q1,
      `...3`
   )

##  Finding Dx -----------------------------------------------------------------

dx <- read_dta(hs_data("harp_dx", "reg", 2024, 5))

award_dx <- dx %>%
   filter(year == 2022) %>%
   group_by(dx_region) %>%
   summarise(
      dx22      = n(),
      unknown22 = sum(transmit == "UNKNOWN")
   ) %>%
   ungroup() %>%
   left_join(
      y  = dx %>%
         filter(year == 2023) %>%
         group_by(dx_region) %>%
         summarise(
            dx23      = n(),
            unknown23 = sum(transmit == "UNKNOWN")
         ) %>%
         ungroup(),
      by = join_by(dx_region)
   ) %>%
   mutate(
      perc22 = unknown22 / dx22,
      perc23 = unknown23 / dx23,
   )

award_dx %>% write_xlsx("H:/20240712_awards-dx.xlsx")

##  Dura Tx --------------------------------------------------------------------

con      <- ohasis$conn("lw")
form_art <- QB$new(con)$
   from("ohasis_warehouse.form_art_bc")$
   whereBetween("VISIT_DATE", c("2022-01-01", "2023-12-31"))$
   whereNotNull("MEDICINE_SUMMARY")$
   select("REC_ID", "PATIENT_ID", "FACI_ID", "SERVICE_FACI", "SERVICE_SUB_FACI", "VISIT_DATE", "CREATED_AT", "UPDATED_AT")$
   get()
dbDisconnect(con)

dura_tx <- form_art %>%
   filter(year(VISIT_DATE) == 2023) %>%
   mutate(
      FACI_ID     = coalesce(SERVICE_FACI, FACI_ID),
      SUB_FACI_ID = SERVICE_SUB_FACI,

      ym          = format(VISIT_DATE, "%Y-%m"),
      start       = as.Date(format(VISIT_DATE, "%Y-%m-01")),
      end         = start %m+% months(1),
      end         = end %m+% days(14),
   ) %>%
   ohasis$get_faci(
      list(site = c("FACI_ID", "SUB_FACI_ID")),
      "nhsss",
      c("reg", "prov", "munc")
   ) %>%
   group_by(reg, ym) %>%
   summarise(
      forms  = n(),
      ontime = sum(as.Date(CREATED_AT) <= end),
      start  = min(start),
      end    = max(end),
   ) %>%
   ungroup()

dura_tx %>%
   mutate(
      `%ontime` = ontime / forms
   ) %>%
   pivot_wider(
      id_cols     = reg,
      names_from  = ym,
      values_from = c(forms, ontime, `%ontime`)
   ) %>%
   write_xlsx("H:/20240712_awards-dura_tx.xlsx")

##  Rated Tx -------------------------------------------------------------------

rated_tx <- form_art %>%
   mutate(
      FACI_ID     = coalesce(SERVICE_FACI, FACI_ID),
      SUB_FACI_ID = SERVICE_SUB_FACI,

      yr          = format(VISIT_DATE, "%Y"),
      start       = as.Date(format(VISIT_DATE, "%Y-%m-01")),
      end         = start %m+% months(1),
      end         = end %m+% days(14),
   ) %>%
   ohasis$get_faci(
      list(site = c("FACI_ID", "SUB_FACI_ID")),
      "nhsss",
      c("reg", "prov", "munc")
   ) %>%
   group_by(reg, yr) %>%
   summarise(
      forms  = n(),
      ontime = sum(as.Date(CREATED_AT) <= end),
      start  = min(start),
      end    = max(end),
   ) %>%
   ungroup()

rated_tx %>%
   mutate(
      `%ontime` = ontime / forms
   ) %>%
   pivot_wider(
      id_cols     = reg,
      names_from  = yr,
      values_from = c(forms, ontime, `%ontime`)
   ) %>%
   write_xlsx("H:/20240712_awards-rated_tx.xlsx")

