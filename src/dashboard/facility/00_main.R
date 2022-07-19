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

dqai$harp$dx <- ohasis$get_data("harp_dx", "2022", "05") %>%
   read_dta() %>%
   zap_missing()

dqai$harp$tx$reg <- ohasis$get_data("harp_tx-reg", "2022", "05") %>%
   read_dta() %>%
   zap_missing() %>%
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
   zap_missing()

##  Consolidate data -----------------------------------------------------------

dqai$coverage$next_date <- ohasis$get_date_ref("next", "2022", "05")
dqai$coverage$next_date <- as.Date(paste(sep = "-", dqai$coverage$next_date$yr, dqai$coverage$next_date$mo, "01"))
dqai$data               <- dqai$forms$form_art_bc %>%
   filter(
      VISIT_DATE < dqai$coverage$next_date
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
      sex,
      uic,
      first,
      middle,
      last,
      suffix,
      px_code,
      age
   ) %>%
   left_join(
      y  = dqai$harp$tx$outcome %>%
         select(
            art_id,
            idnum,
            curr_age,
            hub,
            branch,
            latest_ffupdate,
            latest_nextpickup,
            art_reg,
            outcome
         ),
      by = "art_id"
   ) %>%
   left_join(
      y  = ohasis$ref_faci_code %>%
         mutate(
            FACI_CODE     = case_when(
               stri_detect_regex(SUB_FACI_CODE, "^HASH") ~ "HASH",
               stri_detect_regex(SUB_FACI_CODE, "^SAIL") ~ "SAIL",
               stri_detect_regex(SUB_FACI_CODE, "^TLY") ~ "TLY",
               TRUE ~ FACI_CODE
            ),
            SUB_FACI_CODE = if_else(
               condition = nchar(SUB_FACI_CODE) == 3,
               true      = NA_character_,
               false     = SUB_FACI_CODE
            ),
            SUB_FACI_CODE = case_when(
               FACI_CODE == "HASH" & is.na(SUB_FACI_CODE) ~ "HASH-QC",
               FACI_CODE == "TLY" & is.na(SUB_FACI_CODE) ~ "TLY-ANGLO",
               FACI_CODE == "SHP" & is.na(SUB_FACI_CODE) ~ "SHIP-MAKATI",
               TRUE ~ SUB_FACI_CODE
            ),
         ) %>%
         select(
            hub       = FACI_CODE,
            branch    = SUB_FACI_CODE,
            curr_faci = FACI_NAME,
         ) %>%
         distinct_all(),
      by = c("hub", "branch")
   ) %>%
   left_join(
      y  = dqai$harp$dx %>%
         select(
            idnum,
            region,
            province,
            muncity,
            confirm_date
         ) %>%
         mutate(
            # weird muncity
            muncity = if_else(
               condition = province == "BULACAN" & muncity == "SAN JUAN",
               true      = "MALOLOS",
               false     = muncity,
               missing   = muncity
            )
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
         mutate(
            drop = if_else(
               condition = StrLeft(PSGC_MUNC, 4) == "1339" & PSGC_MUNC != "133900000",
               true      = 1,
               false     = 0,
               missing   = 0
            )
         ) %>%
         filter(drop == 0) %>%
         select(
            region         = NHSSS_REG,
            province       = NHSSS_PROV,
            muncity        = NHSSS_MUNC,
            perm_reg       = NAME_REG,
            perm_prov      = NAME_PROV,
            perm_munc      = NAME_MUNC,
            perm_prov_psgc = PSGC_PROV
         ),
      by = c("region", "province", "muncity")
   ) %>%
   mutate(
      tat_confirm_enroll = floor(interval(confirm_date, artstart_date) / days(1)),
      # tat_confirm_enroll = case_when(
      #    tat_confirm_enroll < 0 ~ "0) Tx before dx",
      #    tat_confirm_enroll == 0 ~ "1) Same day",
      #    tat_confirm_enroll >= 1 & tat_confirm_enroll <= 7 ~ "2) 1 - 7 days",
      #    tat_confirm_enroll >= 8 & tat_confirm_enroll <= 14 ~ "3) 8 - 14 days",
      #    tat_confirm_enroll >= 15 & tat_confirm_enroll <= 30 ~ "4) 15 - 30 days",
      #    tat_confirm_enroll >= 31 ~ "5) More than 30 days",
      #    is.na(confirm_date) ~ "6) More than 30 days",
      #    TRUE ~ "(no confirm date)",
      # ),

      age                = floor(age),
      Age_Band           = case_when(
         age >= 0 & age < 15 ~ '<15',
         age >= 15 & age < 25 ~ '15-24',
         age >= 25 & age < 35 ~ '25-34',
         age >= 35 & age < 50 ~ '35-49',
         age >= 50 & age < 1000 ~ '50+',
         TRUE ~ '(no data)'
      ),
      sex                = case_when(
         sex == "MALE" ~ "Male",
         sex == "FEMALE" ~ "Female",
         TRUE ~ "(no data)"
      ),
      outcome            = case_when(
         outcome == "alive on arv" ~ "Alive on ARV",
         outcome == "lost to follow up" ~ "LTFU",
         outcome == "dead" ~ "Mortality",
         outcome == "stopped - negative" ~ "Stopped",
         TRUE ~ "(no data)"
      ),
   ) %>%
   # left_join(
   #    y  = json_dta %>%
   #       select(
   #          PSGC_PROV,
   #          Contour = path_json
   #       ),
   #    by = "PSGC_PROV"
   # ) %>%
   # select(-PSGC_PROV) %>%
   distinct_all()

##  upsert facility data -------------------------------------------------------

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
# dbxUpsert(lw_conn, schema, full_faci, c("art_id", "faci"), batch_size = 1000)
ohasis$upsert(lw_conn, "harp", "per_faci_art", full_faci, c("art_id", "faci"))
dbDisconnect(lw_conn)

##  geojson data ---------------------------------------------------------------

shell(r"(
ogr2ogr -simplify 0.0005 "H:/_QGIS/geojson_bene/ph_province.geojson" "H:/_QGIS/Shapefiles/Philippines 2020-05-29/phl_admbnda_adm2_psa_namria_20200529.shp"
)")
shell(r"(
geojson-rewind  "H:/_QGIS/geojson_bene/ph_muncity.geojson" > "H:/_QGIS/geojson_bene/ph_muncity_rewind.geojson"
)")

shell(r"(
ogr2ogr -simplify 0.0005 "H:/_QGIS/geojson_bene/ph_muncity.geojson" "H:/_QGIS/Shapefiles/Philippines 2020-05-29/phl_admbnda_adm3_psa_namria_20200529.shp"
)")
shell(r"(
geojson-rewind  "H:/_QGIS/geojson_bene/ph_province.geojson" > "H:/_QGIS/geojson_bene/ph_province_rewind.geojson"
)")

json     <- jsonlite::read_json("H:/_QGIS/geojson_bene/ph_muncity_rewind.geojson")
json_dta <- data.frame()
tag      <- 1
for (i in seq_len(length(json$features))) {
   l1 <- max(lengths(lapply(json$features[[i]]$geometry$coordinates, unlist)))
   for (j in seq_len(length(json$features[[i]]$geometry$coordinates))) {
      for (k in seq_len(length(json$features[[i]]$geometry$coordinates[[j]]))) {
         if (length(json$features[[i]]$geometry$coordinates[[j]][[k]]) == 2) {
            json$features[[i]]$geometry$coordinates[[j]][[k]] <- paste0("[", paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates[[j]][[k]])), "]")
            tag                                               <- 0
         } else {
            for (l in seq_len(length(json$features[[i]]$geometry$coordinates[[j]][[k]]))) {
               json$features[[i]]$geometry$coordinates[[j]][[k]][[l]] <- paste0("[", paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates[[j]][[k]][[l]])), "]")
            }
            tag <- 1

            json$features[[i]]$geometry$coordinates[[j]][[k]] <- paste0("[", paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates[[j]][[k]])), "]")
            orig_len                                          <- length(json$features[[i]]$geometry$coordinates[[j]][[k]])
            # json$features[[i]]$geometry$coordinates[[j]][[k]] <- unlist(jsonlite::toJSON(json$features[[i]]$geometry$coordinates[[j]][[k]]))
            #
            # if (orig_len == (l1 / 2)) {
            #    json$features[[i]]$geometry$coordinates[[j]][[k]] <- substr(
            #       json$features[[i]]$geometry$coordinates[[j]][[k]],
            #       2,
            #       nchar(json$features[[i]]$geometry$coordinates[[j]][[k]]) - 1
            #    )
            # }

            # if (orig_len == (l1 / 2)) {
            #    json$features[[i]]$geometry$coordinates <- json$features[[i]]$geometry$coordinates[[j]][[k]]
            # }
         }
      }
      json$features[[i]]$geometry$coordinates[[j]] <- paste0("[", paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates[[j]])), "]")
   }
   if (length(json$features[[i]]$geometry$coordinates) == 1) {
      df <- tibble(
         ELEM_NUM  = i,
         PSGC_MUNC = substr(json$features[[i]]$properties$ADM3_PCODE, 3, 1000),
         PSGC_NAME = json$features[[i]]$properties$ADM3_EN,
         GEOJSON   = paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates))
      )
   } else {
      df <- tibble(
         ELEM_NUM  = i,
         PSGC_MUNC = substr(json$features[[i]]$properties$ADM3_PCODE, 3, 1000),
         PSGC_NAME = json$features[[i]]$properties$ADM3_EN,
         GEOJSON   = paste0(r"([)", paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates)), r"(])")
      )
   }

   if (nrow(json_dta) == 0)
      json_dta <- df
   else
      json_dta <- bind_rows(json_dta, df)
}
json_dta %<>% distinct_all()

# special manila
tag  <- 1
json <- jsonlite::read_json("H:/_QGIS/geojson_bene/ph_province_rewind.geojson")

for (i in seq_len(length(json$features))) {
   l1 <- max(lengths(lapply(json$features[[i]]$geometry$coordinates, unlist)))
   for (j in seq_len(length(json$features[[i]]$geometry$coordinates))) {
      for (k in seq_len(length(json$features[[i]]$geometry$coordinates[[j]]))) {
         if (length(json$features[[i]]$geometry$coordinates[[j]][[k]]) == 2) {
            json$features[[i]]$geometry$coordinates[[j]][[k]] <- paste0("[", paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates[[j]][[k]])), "]")
            tag                                               <- 0
         } else {
            for (l in seq_len(length(json$features[[i]]$geometry$coordinates[[j]][[k]]))) {
               json$features[[i]]$geometry$coordinates[[j]][[k]][[l]] <- paste0("[", paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates[[j]][[k]][[l]])), "]")
            }
            tag <- 1

            json$features[[i]]$geometry$coordinates[[j]][[k]] <- paste0("[", paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates[[j]][[k]])), "]")
            orig_len                                          <- length(json$features[[i]]$geometry$coordinates[[j]][[k]])
            # json$features[[i]]$geometry$coordinates[[j]][[k]] <- unlist(jsonlite::toJSON(json$features[[i]]$geometry$coordinates[[j]][[k]]))
            #
            # if (orig_len == (l1 / 2)) {
            #    json$features[[i]]$geometry$coordinates[[j]][[k]] <- substr(
            #       json$features[[i]]$geometry$coordinates[[j]][[k]],
            #       2,
            #       nchar(json$features[[i]]$geometry$coordinates[[j]][[k]]) - 1
            #    )
            # }
            #
            # if (orig_len == (l1 / 2)) {
            #    json$features[[i]]$geometry$coordinates <- json$features[[i]]$geometry$coordinates[[j]][[k]]
            # }
         }
      }
      json$features[[i]]$geometry$coordinates[[j]] <- paste0("[", paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates[[j]])), "]")
   }
   if (length(json$features[[i]]$geometry$coordinates) == 1) {
      df <- tibble(
         ELEM_NUM  = i,
         PSGC_MUNC = substr(json$features[[i]]$properties$ADM2_PCODE, 3, 1000),
         PSGC_NAME = json$features[[i]]$properties$ADM2_EN,
         GEOJSON   = paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates))
      )
   } else {
      df <- tibble(
         ELEM_NUM  = i,
         PSGC_MUNC = substr(json$features[[i]]$properties$ADM2_PCODE, 3, 1000),
         PSGC_NAME = json$features[[i]]$properties$ADM2_EN,
         GEOJSON   = paste0(r"([)", paste(collapse = ", ", unlist(json$features[[i]]$geometry$coordinates)), r"(])")
      )
   }

   if (df$PSGC_MUNC == "133900000") {
      print(i)
      json_dta <- bind_rows(json_dta, df)
   }
}
json_dta %<>% distinct_all()

lw_conn <- ohasis$conn("lw")
schema  <- Id(schema = "harp", table = "json_muncity")
if (dbExistsTable(lw_conn, schema))
   dbRemoveTable(lw_conn, schema)

dbCreateTable(lw_conn, schema, json_dta)
dbExecute(lw_conn, r"(
alter table harp.json_muncity
    add constraint json_muncity_pk
        primary key (PSGC_MUNC);
)")
dbExecute(lw_conn, r"(
alter table harp.json_muncity
    modify GEOJSON JSON;
)")
ohasis$upsert(lw_conn, "harp", "json_muncity", json_dta, "PSGC_MUNC")
dbDisconnect(lw_conn)

