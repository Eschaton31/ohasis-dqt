


try <- gf$logsheet$psfi %>%
   filter(sheet %in% c("MSMTGW", "PWID")) %>%
   left_join(
	  y  = gf$corr$logsheet_psfi$staff %>%
		 rename(
			site_region = 2
		 ) %>%
		 select(
			site_region,
			site_province,
			site_muncity,
			ls_subtype,
			provider_name,
			PSFI_STAFF_ID,
			USER_ID,
			FACI_ID,
			SUB_FACI_ID
		 ),
	  by = c(
		 "site_region",
		 "site_province",
		 "site_muncity",
		 "ls_subtype",
		 "provider_name"
	  )
   ) %>%
   mutate(
	  SERVICE_SUB_FACI = case_when(
		 !is.na(FACI_ID) ~ SUB_FACI_ID,
	  ),
	  SERVICE_FACI     = case_when(
		 !is.na(FACI_ID) ~ FACI_ID,
		 !is.na(USER_ID) ~ StrLeft(USER_ID, 6),
	  ),
	  with_uic         = if_else(
		 condition = !is.na(SERVICE_FACI),
		 true      = 1,
		 false     = 0
	  )
   ) %>%
   left_join(
	  y  = ohasis_uic %>%
		 select(
			uic = UIC,
			SERVICE_FACI,
			PATIENT_ID
		 ) %>%
		 distinct_all(),
	  by = c("uic", "SERVICE_FACI")
   ) %>%
   left_join(
	  y  = ohasis_uic %>%
		 select(
			uic     = UIC,
			UIC_PID = PATIENT_ID
		 ) %>%
		 distinct_all(),
	  by = "uic"
   ) %>%
   mutate(
	  with_pid   = case_when(
		 !is.na(PATIENT_ID) ~ 1,
		 !is.na(UIC_PID) ~ 1,
		 TRUE ~ 0
	  ),
	  PATIENT_ID = case_when(
		 !is.na(PATIENT_ID) ~ PATIENT_ID,
		 TRUE ~ UIC_PID
	  )
   ) %>%
   left_join(
	  y  = id_registry, ,
	  by = "PATIENT_ID"
   ) %>%
   distinct(ohasis_record, .keep_all = TRUE) %>%
   mutate(
	  CENTRAL_ID = if_else(
		 condition = is.na(CENTRAL_ID),
		 true      = PATIENT_ID,
		 false     = CENTRAL_ID
	  ),
   )


combined <- bind_rows(
   gf$logsheet$ohasis %>%
	  distinct(ohasis_id, .keep_all = TRUE) %>%
	  anti_join(
		 y  = try %>%
			select(ohasis_id = CENTRAL_ID),
		 by = "ohasis_id"
	  ) %>%
	  mutate(data_src = "OHASIS") %>%
	  mutate_at(
		 .vars = vars(num_sex_partner_m, num_sex_partner_f),
		 ~as.character(.)
	  ),
   try %>%
	  anti_join(
		 y  = gf$logsheet$ohasis %>%
			distinct(ohasis_id, .keep_all = TRUE) %>%
			select(CENTRAL_ID = ohasis_id),
		 by = "CENTRAL_ID"
	  ) %>%
	  rename(ohasis_id = CENTRAL_ID) %>%
	  mutate(data_src = "PSFI"),
   try %>%
	  inner_join(
		 y  = gf$logsheet$ohasis %>%
			distinct(ohasis_id, .keep_all = TRUE) %>%
			select(CENTRAL_ID = ohasis_id),
		 by = "CENTRAL_ID"
	  ) %>%
	  rename(ohasis_id = CENTRAL_ID) %>%
	  mutate(data_src = "Both")
)

combined <- bind_rows(
   gf$logsheet$ohasis %>%
	  distinct(ohasis_id, .keep_all = TRUE) %>%
	  anti_join(
		 y  = try %>%
			select(ohasis_id = CENTRAL_ID),
		 by = "ohasis_id"
	  )
)

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

forma_rr <- gf$logsheet$ohasis %>%
   filter(
	  sextype_anal_insert == "Y" | sextype_anal_receive == "Y",
	  doh_eb_form == "DOH-EB Form: Form A"
   ) %>%
   mutate(
	  is_tested = case_when(
		 reactive == "Y" ~ 1,
		 !is.na(test_date) ~ 1,
		 TRUE ~ 0
	  ),
	  reactive  = case_when(
		 is_tested == 1 & reactive == "Y" ~ 1,
		 TRUE ~ 0
	  ),
   ) %>%
   group_by(site_name, kap_type) %>%
   summarise(
	  reactive = sum(reactive),
	  tested   = sum(is_tested),
   ) %>%
   ungroup() %>%
   mutate(rr = format((reactive / tested) * 100, digits = 2))

hts_rr <- gf$logsheet$ohasis %>%
   filter(
	  sextype_anal_insert == "Y" | sextype_anal_receive == "Y",
	  doh_eb_form == "DOH-EB Form: HTS Form"
   ) %>%
   mutate(
	  is_tested = case_when(
		 reactive == "Y" ~ 1,
		 !is.na(test_date) ~ 1,
		 TRUE ~ 0
	  ),
	  reactive  = case_when(
		 is_tested == 1 & reactive == "Y" ~ 1,
		 TRUE ~ 0
	  ),
   ) %>%
   group_by(site_name, kap_type) %>%
   summarise(
	  reactive = sum(reactive),
	  tested   = sum(is_tested),
   ) %>%
   ungroup() %>%
   mutate(rr = format((reactive / tested) * 100, digits = 2))

cfbs_rr <- gf$logsheet$ohasis %>%
   filter(
	  sextype_anal_insert == "Y" | sextype_anal_receive == "Y",
	  doh_eb_form == "DOH-EB Form: CFBS Form"
   ) %>%
   mutate(
	  is_tested = case_when(
		 reactive == "Y" ~ 1,
		 !is.na(test_date) ~ 1,
		 TRUE ~ 0
	  ),
	  reactive  = case_when(
		 is_tested == 1 & reactive == "Y" ~ 1,
		 TRUE ~ 0
	  ),
   ) %>%
   group_by(site_name, kap_type) %>%
   summarise(
	  reactive = sum(reactive),
	  tested   = sum(is_tested),
   ) %>%
   ungroup() %>%
   mutate(rr = format((reactive / tested) * 100, digits = 2))

oh_rr <- gf$logsheet$ohasis %>%
   filter(
	  sextype_anal_insert == "Y" | sextype_anal_receive == "Y",
   ) %>%
   mutate(
	  is_tested = case_when(
		 reactive == "Y" ~ 1,
		 !is.na(test_date) ~ 1,
		 TRUE ~ 0
	  ),
	  reactive  = case_when(
		 is_tested == 1 & reactive == "Y" ~ 1,
		 TRUE ~ 0
	  ),
   ) %>%
   group_by(site_name, kap_type) %>%
   summarise(
	  reactive = sum(reactive),
	  tested   = sum(is_tested),
   ) %>%
   ungroup() %>%
   mutate(rr = format((reactive / tested) * 100, digits = 2))

write_xlsx(list("CBS RR" = cfbs_rr, "Form A RR" = forma_rr, "HTS Form RR" = hts_rr), "H:/20220520_rr-by-faci.xlsx")

combined %>%
   mutate(
	  is_tested = case_when(
		 reactive == "Y" ~ 1,
		 !is.na(test_date) ~ 1,
		 TRUE ~ 0
	  ),
	  reactive  = case_when(
		 is_tested == 1 & reactive == "Y" ~ 1,
		 TRUE ~ 0
	  ),
   ) %>%
   group_by(kap_type, data_src) %>%
   summarise(
	  reactive = sum(reactive),
	  tested   = sum(is_tested),
   ) %>%
   ungroup()

write_xlsx(
   combined %>%
	  mutate(
		 is_tested = case_when(
			reactive == "Y" ~ 1,
			!is.na(test_date) ~ 1,
			TRUE ~ 0
		 ),
		 reactive  = case_when(
			is_tested == 1 & reactive == "Y" ~ 1,
			TRUE ~ 0
		 ),
	  ) %>%
	  group_by(kap_type, data_src) %>%
	  summarise(
		 reactive = sum(reactive),
		 tested   = sum(is_tested),
	  ) %>%
	  ungroup() %>%
	  mutate(rr = format((reactive / tested) * 100, digits = 2)),
   "H:/20220520_rr-by-kap_src.xlsx"
)

.tab(
   combined %>%
	  filter(sextype_anal_insert == "Y" | sextype_anal_receive == "Y"),
   reactive
)

write_dta(
   gf$logsheet$ohasis %>%
	  filter(with_dsa == 1) %>%
	  select(
		 -starts_with("RISK"),
		 -starts_with("EXPOSE"),
		 RECORD_P12M
	  ),
   "H:/Data Sharing/PSFI/2022.06/ohasis_logsheet_2022-06.dta"
)

write_xlsx(
   gf$logsheet$ohasis %>%
	  filter(with_dsa == 1) %>%
	  select(
		 -starts_with("RISK"),
		 -starts_with("EXPOSE"),
		 RECORD_P12M
	  ),
   "H:/Data Sharing/PSFI/2022.06/ohasis_logsheet_2022-06.xlsx"
)