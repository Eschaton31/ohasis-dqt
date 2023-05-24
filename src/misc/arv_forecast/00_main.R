dr <- list()

sort_harp_region <- function(harp_region) {
   sort <- case_when(
      harp_region == "1" ~ 1,
      harp_region == "2" ~ 2,
      harp_region == "3" ~ 3,
      harp_region == "4A" ~ 4,
      harp_region == "4B" ~ 5,
      harp_region == "5" ~ 6,
      harp_region == "6" ~ 7,
      harp_region == "7" ~ 8,
      harp_region == "8" ~ 9,
      harp_region == "9" ~ 10,
      harp_region == "10" ~ 11,
      harp_region == "11" ~ 12,
      harp_region == "12" ~ 13,
      harp_region == "ARMM" ~ 14,
      harp_region == "CAR" ~ 15,
      harp_region == "CARAGA" ~ 16,
      harp_region == "NCR" ~ 17,
   )

   return(sort)
}

##  Get visits w/in the scope --------------------------------------------------

dr$coverage <- list(min = "2022-04-01", max = "2023-03-31")

lw_conn           <- ohasis$conn("lw")
dr$data$art_forms <- dbTable(
   lw_conn,
   "ohasis_warehouse",
   "form_art_bc",
   cols      = c(
      "REC_ID",
      "PATIENT_ID",
      "VISIT_DATE",
      "MEDICINE_SUMMARY"
   ),
   where     = glue("
      (VISIT_DATE BETWEEN '{dr$coverage$min}' AND '{dr$coverage$max}') AND
         MEDICINE_SUMMARY IS NOT NULL
      "),
   raw_where = TRUE
)
dbDisconnect(lw_conn)
rm(lw_conn)

db_conn        <- ohasis$conn("db")
dr$data$id_reg <- dbTable(
   db_conn,
   "ohasis_interim",
   "registry",
   cols = c(
      "CENTRAL_ID",
      "PATIENT_ID"
   )
)
dbDisconnect(db_conn)
rm(db_conn)

##  Extract regimenm-based groups ----------------------------------------------

dr$data$art_forms %<>% get_cid(dr$data$id_reg, PATIENT_ID)

dr$data$efv <- dr$data$art_forms %>% filter(str_detect(MEDICINE_SUMMARY, "EFV"))
dr$data$nvp <- dr$data$art_forms %>% filter(str_detect(MEDICINE_SUMMARY, "NVP"))
dr$data$abc <- dr$data$art_forms %>% filter(str_detect(MEDICINE_SUMMARY, "ABC"))
dr$data$azt <- dr$data$art_forms %>% filter(str_detect(MEDICINE_SUMMARY, "AZT"))
dr$data$tld <- dr$data$art_forms %>% filter(str_detect(MEDICINE_SUMMARY, "TDF/3TC/DTG"))
dr$data$tle <- dr$data$art_forms %>% filter(str_detect(MEDICINE_SUMMARY, "TDF/3TC/EFV"))

##  Get latest HARP data -------------------------------------------------------

dr$data$harp_tx <- hs_data("harp_tx", "reg", 2023, 3) %>%
   read_dta(
      col_select = c(
         PATIENT_ID,
         REC_ID,
         art_id,
         artstart_hub,
         artstart_branch
      )
   ) %>%
   left_join(
      y  = dr$data$art_forms %>%
         select(
            REC_ID,
            ENROLL_ARV = MEDICINE_SUMMARY
         ),
      by = join_by(REC_ID)
   ) %>%
   select(-REC_ID) %>%
   left_join(
      y  = hs_data("harp_tx", "outcome", 2023, 3) %>%
         read_dta(),
      by = join_by(art_id)
   ) %>%
   left_join(
      y  = hs_data("harp_tx", "outcome", 2022, 3) %>%
         read_dta(
            col_select = c(
               art_id,
               latest_regimen,
               latest_ffupdate
            )
         ) %>%
         rename(
            regimen_prevyr = latest_regimen,
            ffup_prevyr    = latest_ffupdate
         ),
      by = join_by(art_id)
   ) %>%
   get_cid(dr$data$id_reg, PATIENT_ID) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(ENROLL_FACI = "artstart_hub", ENROLL_SUB_FACI = "artstart_branch")
   ) %>%
   # count sail under ship
   mutate(
      sail_clinic = case_when(
         ENROLL_FACI == "040200" ~ 1,
         ENROLL_FACI == "040211" ~ 1,
         ENROLL_FACI == "130748" ~ 1,
         ENROLL_FACI == "130814" ~ 1,
         TRUE ~ 0
      ),
      ENROLL_FACI = case_when(
         sail_clinic == 1 ~ "130025",
         TRUE ~ ENROLL_FACI
      ),
   ) %>%
   ohasis$get_faci(
      list(ENROLL_HUB = c("ENROLL_FACI", "ENROLL_SUB_FACI")),
      "code",
      c("ENROLL_REG", "ENROLL_PROV", "ENROLL_MUNC")
   ) %>%
   mutate(
      enroll_scope = if_else(
         artstart_date %within% interval(dr$coverage$min, dr$coverage$max),
         1,
         0,
         0
      ),
      enroll_tld   = if_else(
         str_detect(ENROLL_ARV, "TDF/3TC/DTG"),
         1,
         0,
         0
      ),
      enrolL_sort  = sort_harp_region(ENROLL_REG),
      hub_sort     = sort_harp_region(tx_reg),
      outcome3mos  = hiv_tx_outcome(outcome, latest_nextpickup, as.Date("2023-03-31"), 3, "months"),
      onart3mos    = if_else(outcome3mos == "alive on arv", 1, 0, 0)
   )


##  Enrolled per region & started on TLD ---------------------------------------

dr$data$harp_tx %>%
   filter(enroll_scope == 1) %>%
   group_by(enrolL_sort, ENROLL_REG) %>%
   summarise(
      enroll_scope = sum(enroll_scope),
      enroll_tld   = sum(enroll_tld),
   ) %>%
   ungroup() %>%
   arrange(enrolL_sort) %>%
   select(-enrolL_sort) %>%
   filter(ENROLL_REG != "UNKNOWN") %>%
   write_clip()

##  Shifted to TLD -------------------------------------------------------------

dr$data$harp_tx %>%
   # generate baseline
   mutate(
      from_efv = if_else(
         str_detect(regimen_prevyr, "EFV") & !str_detect(regimen_prevyr, "TDF/3TC/DTG"),
         1,
         0,
         0
      ),
      from_nvp = if_else(
         str_detect(regimen_prevyr, "NVP") & !str_detect(regimen_prevyr, "TDF/3TC/DTG"),
         1,
         0,
         0
      )
   ) %>%
   left_join(
      y  = dr$data$tld %>%
         select(CENTRAL_ID, VISIT_DATE, MEDICINE_SUMMARY) %>%
         mutate(to_tld = 1),
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      from_efv = if_else(
         from_efv == 1 &
            to_tld == 1 &
            VISIT_DATE > ffup_prevyr,
         1,
         0,
         0
      ),
      from_nvp = if_else(
         from_nvp == 1 &
            to_tld == 1 &
            VISIT_DATE > ffup_prevyr,
         1,
         0,
         0
      )
   ) %>%
   group_by(art_id, hub_sort, tx_reg) %>%
   summarise_at(
      .vars = vars(from_efv, from_nvp),
      ~max(.)
   ) %>%
   ungroup() %>%
   group_by(hub_sort, tx_reg) %>%
   summarise_at(
      .vars = vars(from_efv, from_nvp),
      ~sum(.)
   ) %>%
   ungroup() %>%
   arrange(hub_sort) %>%
   select(-hub_sort) %>%
   write_clip()

##  Shifted from TLD -----------------------------------------------------------

dr$data$harp_tx %>%
   mutate(
      from_tld = if_else(
         str_detect(regimen_prevyr, "TDF/3TC/DTG"),
         1,
         0,
         0
      ),
   ) %>%
   left_join(
      y  = dr$data$art_forms %>%
         select(CENTRAL_ID, VISIT_DATE, MEDICINE_SUMMARY),
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      to_abc = case_when(
         from_tld == 1 &
            str_detect(MEDICINE_SUMMARY, "ABC") &
            !str_detect(MEDICINE_SUMMARY, "TDF/3TC/DTG") &
            VISIT_DATE > ffup_prevyr ~ 1,
         from_tld == 1 &
            str_detect(latest_regimen, "ABC") &
            !str_detect(latest_regimen, "TDF/3TC/DTG") ~ 1,
         TRUE ~ 0
      ),
      to_azt = case_when(
         from_tld == 1 &
            str_detect(MEDICINE_SUMMARY, "AZT") &
            str_detect(MEDICINE_SUMMARY, "TDF/3TC/DTG") &
            VISIT_DATE > ffup_prevyr ~ 1,
         from_tld == 1 &
            str_detect(latest_regimen, "AZT") &
            !str_detect(latest_regimen, "TDF/3TC/DTG") ~ 1,
         TRUE ~ 0
      ),
   ) %>%
   group_by(art_id, hub_sort, tx_reg) %>%
   summarise_at(
      .vars = vars(to_abc, to_azt),
      ~max(.)
   ) %>%
   ungroup() %>%
   group_by(hub_sort, tx_reg) %>%
   summarise_at(
      .vars = vars(to_abc, to_azt),
      ~sum(.)
   ) %>%
   ungroup() %>%
   arrange(hub_sort) %>%
   select(-hub_sort) %>%
   write_clip()

##  Shifted from LTE -----------------------------------------------------------

dr$data$harp_tx %>%
   mutate(
      from_tle = if_else(
         str_detect(regimen_prevyr, "TDF/3TC/EFV"),
         1,
         0,
         0
      ),
   ) %>%
   left_join(
      y  = dr$data$art_forms %>%
         select(CENTRAL_ID, VISIT_DATE, MEDICINE_SUMMARY),
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate(
      to_tld = case_when(
         from_tle == 1 &
            str_detect(MEDICINE_SUMMARY, "TDF/3TC/DTG") &
            !str_detect(MEDICINE_SUMMARY, "TDF/3TC/EFV") &
            VISIT_DATE > ffup_prevyr ~ 1,
         from_tle == 1 &
            str_detect(latest_regimen, "TDF/3TC/DTG") &
            !str_detect(latest_regimen, "TDF/3TC/EFV") ~ 1,
         TRUE ~ 0
      ),
      to_abc = case_when(
         from_tle == 1 &
            str_detect(MEDICINE_SUMMARY, "ABC") &
            !str_detect(MEDICINE_SUMMARY, "TDF/3TC/EFV") &
            VISIT_DATE > ffup_prevyr ~ 1,
         from_tle == 1 &
            str_detect(latest_regimen, "ABC") &
            !str_detect(latest_regimen, "TDF/3TC/EFV") ~ 1,
         TRUE ~ 0
      ),
      to_azt = case_when(
         from_tle == 1 &
            str_detect(MEDICINE_SUMMARY, "AZT") &
            str_detect(MEDICINE_SUMMARY, "TDF/3TC/EFV") &
            VISIT_DATE > ffup_prevyr ~ 1,
         from_tle == 1 &
            str_detect(latest_regimen, "AZT") &
            !str_detect(latest_regimen, "TDF/3TC/EFV") ~ 1,
         TRUE ~ 0
      ),
   ) %>%
   group_by(art_id, hub_sort, tx_reg) %>%
   summarise_at(
      .vars = vars(to_tld, to_abc, to_azt),
      ~max(.)
   ) %>%
   ungroup() %>%
   group_by(hub_sort, tx_reg) %>%
   summarise_at(
      .vars = vars(to_tld, to_abc, to_azt),
      ~sum(.)
   ) %>%
   ungroup() %>%
   arrange(hub_sort) %>%
   select(-hub_sort) %>%
   write_clip()