##  PrEP Linkage Controller ----------------------------------------------------

# define datasets
if (!exists("nhsss"))
   nhsss <- list()

if (!("prep" %in% names(nhsss)))
   nhsss$prep <- new.env()

nhsss$prep$wd <- file.path(getwd(), "src", "official", "prep")

##  Begin linkage of art registry ----------------------------------------------

source(file.path(nhsss$prep$wd, "01_load_reqs.R"))
source(file.path(nhsss$prep$wd, "02_load_visits.R"))
source(file.path(nhsss$prep$wd, "03_data_reg.initial.R"))
source(file.path(nhsss$prep$wd, "04_data_reg.convert.R"))
source(file.path(nhsss$prep$wd, "06_data_reg.final.R"))

##  PII Deduplication ----------------------------------------------------------

# check if deduplications are to be run
dedup <- input(
   prompt  = "Do you want to run the deduplication?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (dedup == "1") {
   source(file.path(nhsss$prep$wd, "07_dedup_new.R"))
   source(file.path(nhsss$prep$wd, "08_dedup_old.R"))
   source(file.path(nhsss$prep$wd, "09_dedup_dx.R"))
}
rm(dedup)

##  Begin linkage of outcomes dataset ------------------------------------------

source(file.path(nhsss$prep$wd, "11_data_outcome.initial.R"))
source(file.path(nhsss$prep$wd, "12_data_outcome.convert.R"))
source(file.path(nhsss$prep$wd, "13_data_outcome.final.R"))

##  Finalize dataset -----------------------------------------------------------

# check if final dataset is to be completed
complete <- input(
   prompt  = "Do you want to finalize the dataset?",
   options = c("1" = "yes", "2" = "no"),
   default = "2"
)
if (complete == "1") {
   source(file.path(nhsss$prep$wd, "14_output.R"))

   # TODO: Place these after pdf & ml conso
   source(file.path(nhsss$prep$wd, "15_archive.R"))
   source(file.path(nhsss$prep$wd, "16_upload.R"))
}
rm(complete)

## clean gelo's original feb dataset
lw_conn     <- ohasis$conn("lw")
id_registry <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "id_registry")) %>%
   select(CENTRAL_ID, PATIENT_ID) %>%
   collect()
dbDisconnect(lw_conn)

raw  <- read_dta("H:/Endorsements/20220315_gelo/prep_registry_20220310.dta") %>%
   rename_all(toupper) %>%
   mutate_if(
      .predicate = is.character,
      ~if_else(. == "", NA_character_, .)
   )
data <- raw %>%
   select(-starts_with("CENTRAL_ID")) %>%
   select(-starts_with("PREP_FACI_CODE")) %>%
   left_join(
      y  = id_registry,
      by = "PATIENT_ID"
   ) %>%
   rename(
      FACI_ID          = RECORD_FACI,
      SERVICE_FACI     = PREP_FACI,
      SERVICE_SUB_FACI = PREP_SUB_FACI
   ) %>%
   mutate(
      SERVICE_SUB_FACI = case_when(
         StrLeft(SERVICE_FACI, 6) == "030001" & SERVICE_SUB_FACI == "JBLMGH Bahay LInGAD" ~ "030001_001",
         StrLeft(SERVICE_FACI, 6) == "030001" & SERVICE_SUB_FACI == "JBLMGH rHIVda Lab" ~ "030001_002",
         StrLeft(SERVICE_FACI, 6) == "030005" & SERVICE_SUB_FACI == "BGHMC Department of Pathology and Laboratory" ~ "030004_002",
         StrLeft(SERVICE_FACI, 6) == "030005" & SERVICE_SUB_FACI == "BGHMC Bataan Haven" ~ "030005_001",
         StrLeft(SERVICE_FACI, 6) == "030010" & SERVICE_SUB_FACI == "Marilao RHU I - Kanaryong Silungan" ~ "030010_001",
         StrLeft(SERVICE_FACI, 6) == "030010" & SERVICE_SUB_FACI == "Marilao RHU I - TB DOTS Center" ~ "030010_002",
         StrLeft(SERVICE_FACI, 6) == "030016" & SERVICE_SUB_FACI == "PJGMRMC Sanctuario De Paulino" ~ "030016_001",
         StrLeft(SERVICE_FACI, 6) == "030016" & SERVICE_SUB_FACI == "PJGMRMC Department of Pathology and Laboratories" ~ "030016_002",
         StrLeft(SERVICE_FACI, 6) == "030016" & SERVICE_SUB_FACI == "PJGMRMC TB DOTS PMDT" ~ "030016_003",
         StrLeft(SERVICE_FACI, 6) == "040200" & SERVICE_SUB_FACI == "SAIL - Trece Martires" ~ "040200_001",
         StrLeft(SERVICE_FACI, 6) == "040200" & SERVICE_SUB_FACI == "SAIL-Makati" ~ "040200_002",
         StrLeft(SERVICE_FACI, 6) == "040200" & SERVICE_SUB_FACI == "Makati" ~ "040200_002",
         StrLeft(SERVICE_FACI, 6) == "060049" & SERVICE_SUB_FACI == "FPOP Rajah Community Center" ~ "060049_001",
         StrLeft(SERVICE_FACI, 6) == "060049" & SERVICE_SUB_FACI == "FPOP kNOwTell" ~ "060049_002",
         StrLeft(SERVICE_FACI, 6) == "110005" & SERVICE_SUB_FACI == "Davao RHWC HACT" ~ "110005_001",
         StrLeft(SERVICE_FACI, 6) == "110005" & SERVICE_SUB_FACI == "Davao RHWC rHIVda Laboratory" ~ "110005_002",
         StrLeft(SERVICE_FACI, 6) == "130001" & SERVICE_SUB_FACI == "TLY Anglo" ~ "130001_001",
         StrLeft(SERVICE_FACI, 6) == "130001" & SERVICE_SUB_FACI == "TLY Victoria" ~ "130001_002",
         StrLeft(SERVICE_FACI, 6) == "130001" & SERVICE_SUB_FACI == "TLY Hero" ~ "130001_003",
         StrLeft(SERVICE_FACI, 6) == "130001" & SERVICE_SUB_FACI == "TLY Welcome" ~ "130001_004",
         StrLeft(SERVICE_FACI, 6) == "130001" & SERVICE_SUB_FACI == "TLY Athena" ~ "130001_005",
         StrLeft(SERVICE_FACI, 6) == "130001" & SERVICE_SUB_FACI == "TLY Lily" ~ "130001_006",
         StrLeft(SERVICE_FACI, 6) == "130001" & SERVICE_SUB_FACI == "TLY Uni" ~ "130001_007",
         StrLeft(SERVICE_FACI, 6) == "130001" & SERVICE_SUB_FACI == "TLY LuxeCare" ~ "130001_008",
         StrLeft(SERVICE_FACI, 6) == "130003" & SERVICE_SUB_FACI == "RITM HIV/AIDS Research Group (ARG) Clinic" ~ "130003_001",
         StrLeft(SERVICE_FACI, 6) == "130003" & SERVICE_SUB_FACI == "RITM rHIVda Laboratory" ~ "130003_002",
         StrLeft(SERVICE_FACI, 6) == "130005" & SERVICE_SUB_FACI == "Marikina CHO Social Hygiene Clinic" ~ "130005_001",
         StrLeft(SERVICE_FACI, 6) == "130005" & SERVICE_SUB_FACI == "Marikina CHO Public Health Laboratory" ~ "130005_002",
         StrLeft(SERVICE_FACI, 6) == "130005" & SERVICE_SUB_FACI == "Marikina CHO Satellite Treatment Hub" ~ "130005_003",
         StrLeft(SERVICE_FACI, 6) == "130026" & SERVICE_SUB_FACI == "Taguig Clinical Laboratory" ~ "130026_001",
         StrLeft(SERVICE_FACI, 6) == "130026" & SERVICE_SUB_FACI == "Taguig Social Hygiene Clinic" ~ "130026_002",
         StrLeft(SERVICE_FACI, 6) == "130026" & SERVICE_SUB_FACI == "Taguig Drop-in Center" ~ "130026_003",
         StrLeft(SERVICE_FACI, 6) == "130605" & SERVICE_SUB_FACI == "HASH Caloocan Site" ~ "130605_001",
         StrLeft(SERVICE_FACI, 6) == "130605" & SERVICE_SUB_FACI == "HASH Quezon City Site" ~ "130605_002",
         StrLeft(SERVICE_FACI, 6) == "130605" & SERVICE_SUB_FACI == "HASH Makati Site" ~ "130605_003",
         StrLeft(SERVICE_FACI, 6) == "130605" & SERVICE_SUB_FACI == "HASH Trece Martires Site" ~ "130605_004",
         StrLeft(SERVICE_FACI, 6) == "130605" & SERVICE_SUB_FACI == "HASH Calamba Site" ~ "130605_005",
         StrLeft(SERVICE_FACI, 6) == "130605" & SERVICE_SUB_FACI == "GILEAD" ~ "130605_006",
         StrLeft(SERVICE_FACI, 6) == "130605" & SERVICE_SUB_FACI == "TLF Share" ~ "130605_007",
         StrLeft(SERVICE_FACI, 6) == "130605" & SERVICE_SUB_FACI == "Marlon" ~ "130605_008",
         StrLeft(SERVICE_FACI, 6) == "130605" & SERVICE_SUB_FACI == "Ronel" ~ "130605_009",
         StrLeft(SERVICE_FACI, 6) == "130605" & SERVICE_SUB_FACI == "Genesis" ~ "130605_010",
         TRUE ~ SERVICE_SUB_FACI
      ),
      SUB_FACI_ID      = NA_character_,
      PHILSYS_ID       = NA_character_
   ) %>%
   mutate_at(
      .vars = vars(FACI_ID, SERVICE_FACI),
      ~if_else(!is.na(.), StrLeft(., 6), .)
   ) %>%
   # arv disp data
   mutate(
      VISIT_DATE = case_when(
         RECORD_DATE == as.Date(DISP_DATE) ~ RECORD_DATE,
         RECORD_DATE < as.Date(DISP_DATE) & DISP_DATE >= -25567 ~ as.Date(DISP_DATE),
         RECORD_DATE > as.Date(DISP_DATE) & DISP_DATE >= -25567 ~ as.Date(DISP_DATE),
         is.na(RECORD_DATE) ~ as.Date(DISP_DATE),
         is.na(DISP_DATE) ~ RECORD_DATE,
         TRUE ~ RECORD_DATE
      ),
      .before    = RECORD_DATE
   ) %>%
   arrange(CREATED_AT, VISIT_DATE, REC_ID)