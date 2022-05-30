##  GF Logsheet Controller -----------------------------------------------------

# define datasets
if (!exists("gf"))
   gf <- new.env()

gf$wd <- file.path(getwd(), "src", "official", "dsa_gf")

##  Begin linkage of datasets --------------------------------------------------

# update latest art visits references
source("src/official/loghsset/03_load_visits.R")

source(file.path(gf$wd, "01_load_reqs.R"))
source(file.path(gf$wd, "02_loghseet_psfi.R"))
source(file.path(gf$wd, "03_loghseet_ohasis.R"))

faci_users <- ohasis$ref_staff %>%
   mutate(
      FACI_ID     = StrLeft(STAFF_ID, 6),
      SUB_FACI_ID = NA_character_
   )

faci_users <- ohasis$get_faci(
   faci_users,
   list("user_faci" = c("FACI_ID", "SUB_FACI_ID")),
   "code",
   c("site_region", "site_province", "site_muncity")
)
View(
   faci_users %>%
      select(
         STAFF_ID,
         STAFF_NAME,
         starts_with("site")
      ) %>%
      mutate_all(~toupper(.)) %>%
      distinct_all(),
   "users"
)
View(
   ohasis$ref_faci %>%
      select(FACI_NAME, FACI_ID, contains("NHSSS")),
   "faci"
)

psfi_staff <- read_sheet("1qR9sp9VjwGO23vVEr4rMukX6jhDilCa3dbAxcsm2eFI", sheet = "psfi_reference") %>%
   mutate(Region = as.character(Region),
          Name   = str_squish(toupper(Name)))
for_match  <- read_sheet("1qR9sp9VjwGO23vVEr4rMukX6jhDilCa3dbAxcsm2eFI", sheet = "staff") %>%
   filter(is.na(USER_ID))

df <- for_match %>%
   mutate(
      Region = case_when(
         site_region == "CALABARZON" ~ "4A",
         site_region == "MIMAROPA" ~ "4B",
         TRUE ~ stri_replace_all_fixed(site_region, "REGION ", "")
      )
   ) %>%
   rename(Name = provider_name)

df_faci <- faci_users %>%
   select(
      STAFF_ID,
      Name = STAFF_NAME,
      starts_with("site")
   ) %>%
   mutate_all(~toupper(.)) %>%
   distinct_all()

reclink_df <- fastLink(
   dfA              = df,
   dfB              = df_faci,
   varnames         = "Name",
   stringdist.match = "Name",
   partial.match    = "Name",
   # threshold.match  = 0.95,
   cut.a            = 0.90,
   cut.p            = 0.85,
   dedupe.matches   = FALSE,
   n.cores          = 4,

)

if (length(reclink_df$matches$inds.a) > 0) {
   reclink_matched <- getMatches(
      dfA         = df,
      dfB         = df_faci,
      fl.out      = reclink_df,
      combine.dfs = FALSE
   )

   reclink_review <- reclink_matched$dfA.match %>%
      mutate(
         MATCH_ID = row_number()
      ) %>%
      select(
         MATCH_ID,
         row_id,
         Name,
         site_muncity,
         site_province,
         posterior
      ) %>%
      left_join(
         y  = reclink_matched$dfB.match %>%
            mutate(
               MATCH_ID = row_number()
            ) %>%
            select(
               MATCH_ID,
               PSFI_STAFF_ID,
               PSFI_NAME = Name,
               Region,
               `Deployment Facility`,
               posterior
            ),
         by = "MATCH_ID"
      ) %>%
      select(-posterior.y) %>%
      rename(posterior = posterior.x) %>%
      arrange(desc(posterior)) %>%
      relocate(posterior, .before = MATCH_ID) %>%
      # Additional sift through of matches
      mutate(
         # levenshtein
         LV       = stringdist::stringsim(Name, PSFI_NAME, method = 'lv'),
         # jaro-winkler
         JW       = stringdist::stringsim(Name, PSFI_NAME, method = 'jw'),
         # qgram
         QGRAM    = stringdist::stringsim(Name, PSFI_NAME, method = 'qgram', q = 3),
         AVG_DIST = (LV + QGRAM + JW) / 3,
      ) %>%
      # choose 60% and above match
      filter(AVG_DIST >= 0.60, !is.na(posterior))

}