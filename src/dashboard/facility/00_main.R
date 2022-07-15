dqai <- new.env()

##  Forms ----------------------------------------------------------------------

lw_conn <- ohasis$conn("lw")

dqai$forms$form_art_bc <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_art_bc")) %>%
   filter(
	  ART_RECORD == "ART"
   ) %>%
   select(
	  REC_ID,
	  PATIENT_ID,
	  VISIT_DATE,
	  FACI_ID,
	  SERVICE_FACI,
	  SERVICE_SUB_FACI,
	  FACI_DISP,
	  SUB_FACI_DISP,
	  DISP_DATE,
	  RECORD_DATE
   ) %>%
   collect()

dqai$forms$id_registry <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "id_registry")) %>%
   select(CENTRAL_ID, PATIENT_ID) %>%
   collect()

dbDisconnect(lw_conn)

##  HARP -----------------------------------------------------------------------

dqai$harp$tx$reg <- ohasis$get_data("harp_tx-reg", "2022", "05") %>%
   read_dta() %>%
   mutate_if(
	  .predicate = is.character,
	  ~zap_empty(.)
   ) %>%
   select(-CENTRAL_ID) %>%
   left_join(
	  y  = dqai$forms$id_registry,
	  by = "PATIENT_ID"
   ) %>%
   mutate(
	  CENTRAL_ID = if_else(
		 condition = is.na(CENTRAL_ID),
		 true      = PATIENT_ID,
		 false     = CENTRAL_ID
	  ),
   )

dqai$harp$tx$outcome <- ohasis$get_data("harp_tx-outcome", "2022", "05") %>%
   read_dta() %>%
   mutate_if(
	  .predicate = is.character,
	  ~zap_empty(.)
   )

##  Consolidate data -----------------------------------------------------------

dqai$data <- dqai$forms$form_art_bc %>%
   filter(
	  VISIT_DATE < ohasis$next_date
   ) %>%
   left_join(
	  y  = dqai$forms$id_registry,
	  by = "PATIENT_ID"
   ) %>%
   mutate(
	  CENTRAL_ID      = if_else(
		 condition = is.na(CENTRAL_ID),
		 true      = PATIENT_ID,
		 false     = CENTRAL_ID
	  ),

	  # tag those without ART_FACI
	  use_record_faci = if_else(
		 condition = is.na(SERVICE_FACI),
		 true      = 1,
		 false     = 0
	  ),
	  SERVICE_FACI    = if_else(
		 condition = use_record_faci == 1,
		 true      = FACI_ID,
		 false     = SERVICE_FACI
	  ),
   )

dqai$data <- ohasis$get_faci(
   dqai$data,
   list("FACI" = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
   "code"
)

dqai$data %<>%
   mutate(
	  BRANCH = if_else(
		 condition = nchar(FACI) > 4,
		 true      = FACI,
		 false     = NA_character_
	  ),
	  BRANCH = if_else(
		 condition = FACI == "TLY",
		 true      = "TLY-ANGLO",
		 false     = BRANCH,
		 missing   = BRANCH
	  ),
	  FACI   = case_when(
		 stri_detect_regex(BRANCH, "^TLY") ~ "TLY",
		 stri_detect_regex(BRANCH, "^SAIL") ~ "SAIL",
		 TRUE ~ FACI
	  )
   ) %>%
   left_join(
	  y  = dqai$harp$tx$reg %>%
		 select(CENTRAL_ID, art_id),
	  by = "CENTRAL_ID"
   ) %>%
   filter(!is.na(art_id)) %>%
   distinct(art_id, REC_ID, VISIT_DATE, .keep_all = TRUE)

##  Create per faci data -------------------------------------------------------

dqai$faci <- new.env()

invisible(lapply(unique(dqai$data$FACI), function(faci_code) {
   dqai$faci[[faci_code]] <- dqai$data %>%
	  filter(FACI == faci_code) %>%
	  bind_rows(
		 dqai$harp$tx$reg %>%
			filter(artstart_hub == faci_code) %>%
			select(art_id),
		 dqai$harp$tx$outcome %>%
			filter(realhub == faci_code | hub == faci_code)
	  ) %>%
	  distinct(art_id) %>%
	  left_join(
		 y  = dqai$harp$tx$reg %>%
			select(
			   artstart_hub,
			   art_id,
			   idnum,
			   confirmatory_code,
			   px_code,
			   uic,
			   first,
			   middle,
			   last,
			   suffix,
			   age,
			   birthdate,
			   sex,
			   initials,
			   philsys_id,
			   philhealth_no,
			   artstart_date,
			),
		 by = "art_id"
	  ) %>%
	  left_join(
		 y  = dqai$harp$tx$outcome %>%
			select(
			   art_id,
			   curr_age,
			   latest_ffupdate,
			   latest_nextpickup,
			   realhub,
			   realhub_branch,
			   outcome
			),
		 by = "art_id"
	  ) %>%
	  left_join(
		 y  = dqai$data %>%
			select(
			   art_id,
			   artstart_date = VISIT_DATE,
			   HUB_START     = FACI,
			) %>%
			arrange(artstart_date) %>%
			distinct(art_id, .keep_all = TRUE),
		 by = c("artstart_date", "art_id")
	  ) %>%
	  mutate(
		 artstart_hub = if_else(
			condition = is.na(artstart_hub),
			true      = HUB_START,
			false     = artstart_hub,
			missing   = artstart_hub
		 )
	  ) %>%
	  select(-HUB_START) %>%
	  arrange(art_id) %>%
	  filter(!is.na(confirmatory_code)) %>%
	  mutate(HUB = faci_code)
}))

##  Generate final dataset -----------------------------------------------------

full_faci <- bind_rows(as.list(dqai$faci)) %>%
   select(
	  faci = HUB,
	  art_id,
	  artstart_hub,
	  artstart_date,
	  sex
   ) %>%
   left_join(
	  y  = dqai$harp$tx$outcome %>%
		 select(
			art_id,
			idnum,
			curr_age,
			hub,
			latest_ffupdate,
			latest_nextpickup,
			art_reg,
			outcome
		 ),
	  by = "art_id"
   ) %>%
   left_join(
	  y  = dqai$harp$dx %>%
		 select(
			idnum,
			region,
			province,
			muncity,
			confirm_date
		 ),
	  by = "idnum"
   ) %>%
   left_join(
	  y  = ohasis$ref_faci_code %>%
		 filter(is.na(SUB_FACI_CODE) | SUB_FACI_CODE %in% c("TLY-ANGLO", "SHIP-MAKATI", "SAIL-MAKATI")) %>%
		 select(
			faci    = FACI_CODE,
			tx_faci = FACI_NAME,
			tx_reg  = FACI_NAME_REG,
			tx_prov = FACI_NAME_PROV,
			tx_munc = FACI_NAME_MUNC,
		 ) %>%
		 distinct_all(),
	  by = "faci"
   ) %>%
   left_join(
	  y  = ohasis$ref_addr %>%
		 select(
			region    = NHSSS_REG,
			province  = NHSSS_PROV,
			muncity   = NHSSS_MUNC,
			perm_reg  = NAME_REG,
			perm_prov = NAME_PROV,
			perm_munc = NAME_MUNC,
		 ),
	  by = c("region", "province", "muncity")
   ) %>%
   mutate(
	  sex           = case_when(
		 sex == "MALE" ~ "Male",
		 sex == "FEMALE" ~ "Female",
		 TRUE ~ "(no data)"
	  ),
	  outcome       = case_when(
		 outcome == "alive on arv" ~ "Alive on ARV",
		 outcome == "lost to follow up" ~ "LTFU",
		 outcome == "dead" ~ "Mortality",
		 outcome == "stopped - negative" ~ "Stopped",
		 TRUE ~ "(no data)"
	  ),

	  # geo data
	  perm_prov_geo = case_when(
		 perm_prov == "Abra" ~ "PH-ABR",
		 perm_prov == "Agusan del Norte" ~ "PH-AGN",
		 perm_prov == "Agusan del Sur" ~ "PH-AGS",
		 perm_prov == "Aklan" ~ "PH-AKL",
		 perm_prov == "Albay" ~ "PH-ALB",
		 perm_prov == "Antique" ~ "PH-ANT",
		 perm_prov == "Apayao" ~ "PH-APA",
		 perm_prov == "Aurora" ~ "PH-AUR",
		 perm_prov == "Basilan" ~ "PH-BAS",
		 perm_prov == "City of Isabela (Not a Province)" ~ "PH-BAS",
		 perm_prov == "Bataan" ~ "PH-BAN",
		 perm_prov == "Batanes" ~ "PH-BTN",
		 perm_prov == "Batangas" ~ "PH-BTG",
		 perm_prov == "Benguet" ~ "PH-BEN",
		 perm_prov == "Biliran" ~ "PH-BIL",
		 perm_prov == "Bohol" ~ "PH-BOH",
		 perm_prov == "Bukidnon" ~ "PH-BUK",
		 perm_prov == "Bulacan" ~ "PH-BUL",
		 perm_prov == "Cagayan" ~ "PH-CAG",
		 perm_prov == "Camarines Norte" ~ "PH-CAN",
		 perm_prov == "Camarines Sur" ~ "PH-CAS",
		 perm_prov == "Camiguin" ~ "PH-CAM",
		 perm_prov == "Capiz" ~ "PH-CAP",
		 perm_prov == "Catanduanes" ~ "PH-CAT",
		 perm_prov == "Cavite" ~ "PH-CAV",
		 perm_prov == "Cebu" ~ "PH-CEB",
		 perm_prov == "Cotabato" ~ "PH-NCO",
		 perm_prov == "City of Cotabato (Not a Province)" ~ "PH-NCO",
		 perm_prov == "Davao de Oro" ~ "PH-COM",
		 perm_prov == "Davao del Norte" ~ "PH-DAV",
		 perm_prov == "Davao del Sur" ~ "PH-DAS",
		 perm_prov == "Davao Occidental" ~ "PH-DVO",
		 perm_prov == "Davao Oriental" ~ "PH-DAO",
		 perm_prov == "Dinagat Islands" ~ "PH-DIN",
		 perm_prov == "Eastern Samar" ~ "PH-EAS",
		 perm_prov == "Guimaras" ~ "PH-GUI",
		 perm_prov == "Ifugao" ~ "PH-IFU",
		 perm_prov == "Ilocos Norte" ~ "PH-ILN",
		 perm_prov == "Ilocos Sur" ~ "PH-ILS",
		 perm_prov == "Iloilo" ~ "PH-ILI",
		 perm_prov == "Isabela" ~ "PH-ISA",
		 perm_prov == "Kalinga" ~ "PH-KAL",
		 perm_prov == "La Union" ~ "PH-LUN",
		 perm_prov == "Laguna" ~ "PH-LAG",
		 perm_prov == "Lanao del Norte" ~ "PH-LAN",
		 perm_prov == "Lanao del Sur" ~ "PH-LAS",
		 perm_prov == "Leyte" ~ "PH-LEY",
		 perm_prov == "Maguindanao" ~ "PH-MAG",
		 perm_prov == "Marinduque" ~ "PH-MAD",
		 perm_prov == "Masbate" ~ "PH-MAS",
		 perm_prov == "Occidental Mindoro" ~ "PH-MDC",
		 perm_prov == "Oriental Mindoro" ~ "PH-MDR",
		 perm_prov == "Misamis Occidental" ~ "PH-MSC",
		 perm_prov == "Misamis Oriental" ~ "PH-MSR",
		 perm_prov == "Mountain Province" ~ "PH-MOU",
		 perm_prov == "Negros Occidental" ~ "PH-NEC",
		 perm_prov == "Negros Oriental" ~ "PH-NER",
		 perm_prov == "Northern Samar" ~ "PH-NSA",
		 perm_prov == "Nueva Ecija" ~ "PH-NUE",
		 perm_prov == "Nueva Vizcaya" ~ "PH-NUV",
		 perm_prov == "Palawan" ~ "PH-PLW",
		 perm_prov == "Pampanga" ~ "PH-PAM",
		 perm_prov == "Pangasinan" ~ "PH-PAN",
		 perm_prov == "Quezon" ~ "PH-QUE",
		 perm_prov == "Quirino" ~ "PH-QUI",
		 perm_prov == "Rizal" ~ "PH-RIZ",
		 perm_prov == "Romblon" ~ "PH-ROM",
		 perm_prov == "Samar" ~ "PH-WSA",
		 perm_prov == "Sarangani" ~ "PH-SAR",
		 perm_prov == "Siquijor" ~ "PH-SIG",
		 perm_prov == "Sorsogon" ~ "PH-SOR",
		 perm_prov == "South Cotabato" ~ "PH-SCO",
		 perm_prov == "Southern Leyte" ~ "PH-SLE",
		 perm_prov == "Sultan Kudarat" ~ "PH-SUK",
		 perm_prov == "Sulu" ~ "PH-SLU",
		 perm_prov == "Surigao del Norte" ~ "PH-SUN",
		 perm_prov == "Surigao del Sur" ~ "PH-SUR",
		 perm_prov == "Tarlac" ~ "PH-TAR",
		 perm_prov == "Tawi-Tawi" ~ "PH-TAW",
		 perm_prov == "Zambales" ~ "PH-ZMB",
		 perm_prov == "Zamboanga del Norte" ~ "PH-ZAN",
		 perm_prov == "Zamboanga del Sur" ~ "PH-ZAS",
		 perm_prov == "Zamboanga Sibugay" ~ "PH-ZSI",
		 stri_detect_fixed(perm_prov, "NCR") ~ "PH-MNL",
	  )
   )

lw_conn <- ohasis$conn("lw")
schema  <- Id(schema = "harp", table = "per_faci_art")
if (dbExistsTable(lw_conn, schema))
   dbRemoveTable(lw_conn, schema)

dbCreateTable(lw_conn, schema, full_faci)
dbExecute(lw_conn, r"(
alter table harp.per_faci_art
    add constraint per_faci_art_pk
        primary key (faci, art_id);
)")
# dbExecute(lw_conn, r"(
# alter table harp.per_faci_art
#     modify path_json JSON;
# )")
dbxUpsert(lw_conn, schema, full_faci, c("art_id", "faci"), batch_size = 1000)
dbDisconnect(lw_conn)

# json_dta <- data.frame()
# for (i in seq_len(length(json$features))) {
#    df <- json$features[[i]]$properties %>%
# 	  as.data.frame() %>%
# 	  mutate(
# 		 path_json = jsonlite::toJSON(json$features[[i]]$geometry)
# 	  )
#
#    if (nrow(json_dta) == 0)
# 	  json_dta <- df
#    else
# 	  json_dta <- bind_rows(json_dta, df)
# }
#
