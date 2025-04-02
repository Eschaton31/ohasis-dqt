dir     <- "H:/ly-imports"
version <- "20250325"

basePath <- file.path(dir, version)

artPath <- file.path(basePath, "art")
arvPath <- file.path(basePath, "ARV Dispensing Monitoring 2025 (ALL SITES).xlsx")

readArtDb <- function(artPath) {
   files       <- list.files(artPath, full.names = TRUE)
   data        <- lapply(files, read.xlsx, sheet = "Client Information", fillMergedCells = TRUE, skipEmptyCols = TRUE, skipEmptyRows = TRUE, colNames = TRUE, detectDates = FALSE)
   data        <- lapply(data, mutate_all, as.character)
   data        <- lapply(data, mutate_all, toupper)
   data        <- lapply(data, mutate_all, ~na_if(., ""))
   data        <- lapply(data, mutate_all, ~na_if(., "0"))
   data        <- lapply(data, mutate_all, ~na_if(., "-"))
   data        <- lapply(data, mutate_all, ~na_if(., "N/A"))
   data        <- lapply(data, remove_empty, which = "rows")
   names(data) <- tools::file_path_sans_ext(basename(files))


   combined <- data %>%
      bind_rows(.id = "src") %>%
      mutate(row_id = row_number()) %>%
      select(
         row_id,
         BRANCH           = `Hub`,
         FILE             = `src`,
         STATUS           = `Status`,
         LY_STARTED_TX    = `TLY.Started.Treatment`,
         PATIENT_CODE     = `Client.Code`,
         UIC              = `UIC`,
         ACCESSION_CODE   = `Accession.Code`,
         LAST             = `Legal.Surname`,
         FIRST            = `Legal.First.Name`,
         MIDDLE           = `Legal.Middle.Name`,
         SUFFIX           = `Suffix`,
         NICKNAME         = `Preferred.Name`,
         BIRTHDATE_AUTO   = `Date.Of.Birth.MM/DD/YYYY.(Auto)`,
         SEX              = `Sex.At.Birth`,
         SELF_IDENT       = `Gender.Identity`,
         CURR_ADDR        = `Home.Address`,
         WORK_ADDR        = `Work.Address`,
         IS_PREGNANT      = `Preg.on.start`,
         CLIENT_MOBILE    = `Contact.#`,
         CLIENT_EMAIL     = `Email.Address`,
         COUNSELOR        = `Life.Coach./.Counselor`,
         PHILHEALTH_NO    = `Philhealth.#`,
         BIRTHDATE_MANUAL = `Date.Of.Birth.MM/DD/YYYY`,
      ) %>%
      mutate(
         BIRTHDATE = excel_numeric_to_date(parse_number(coalesce(BIRTHDATE_MANUAL, BIRTHDATE_AUTO))),
         .after    = UIC
      )

   return(combined)
}

readArvDisp <- function(arvPath) {
   sheets      <- excel_sheets(arvPath)
   sheets      <- sheets[sheets != 'TEMPLATE']
   sheets      <- sheets[sheets != 'DataImport']
   data        <- lapply(sheets, read.xlsx, xlsxFile = arvPath, fillMergedCells = TRUE, skipEmptyCols = TRUE, skipEmptyRows = TRUE, colNames = TRUE, detectDates = FALSE)
   data        <- lapply(data, mutate_all, as.character)
   data        <- lapply(data, mutate_all, toupper)
   data        <- lapply(data, mutate_all, ~na_if(., ""))
   data        <- lapply(data, mutate_all, ~na_if(., "0"))
   data        <- lapply(data, mutate_all, ~na_if(., "-"))
   data        <- lapply(data, mutate_all, ~na_if(., "---"))
   data        <- lapply(data, mutate_all, ~na_if(., "N/A"))
   data        <- lapply(data, mutate_all, ~na_if(., "#N/A"))
   data        <- lapply(data, mutate_all, ~na_if(., "#REF!"))
   data        <- lapply(data, mutate_all, ~na_if(., "#VALUE!"))
   data        <- lapply(data, remove_empty, which = "rows")
   names(data) <- sheets


   combined <- data %>%
      bind_rows(.id = "BRANCH") %>%
      mutate(row_id = row_number()) %>%
      select(
         row_id,
         BRANCH          = `BRANCH`,
         DISP_DATE       = `DATE.DISPENSED`,
         PATIENT_CODE    = `CLIENT.CODE`,
         STATUS          = `CLIENT.STATUS`,
         UIC             = `UNIQUE.IDENTIFIER.CODE.(UIC)`,
         PHILHEALTH_NO   = `PHILHEALTH.NUMBER`,
         VISIT_TYPE      = `VISIT.TYPE`,
         TB_SCREEN       = `TB.SYMPTOMS`,
         TB_IPT_STATUS   = `TPT.STATUS`,
         TX_STATUS       = `ART.STATUS`,
         ARV_REGIMEN     = `REGIMEN.ON.FILE`,
         OTHER_REGIMEN   = `OTHER.MEDICATIONS.ON.FILE`,
         ARV_DISP        = `MEDS.GIVEN.(PLEASE.INPUT)`,
         CLIENT_TYPE     = `DISPENSING.MODALITY`,
         DISP_TOTAL      = `PILL.DISPENSED`,
         PER_DAY         = `PILLS.PER.DAY`,
         MEDICINE_MISSED = `MISSED.PILLS`,
         MEDICINE_LEFT   = `PILLS.LEFT`,
         NEXT_DATE       = `NEXT.REFILL`,
         REMARKS         = `REMARKS`,
         NAME            = `NAME`,
         HUB_ORIGIN      = `HUB.OF.ORIGIN`,
         REGIMEN         = `REGIMEN`,
      ) %>%
      filter(!if_all(c(DISP_DATE, PATIENT_CODE), ~is.na(.))) %>%
      mutate_at(
         .vars = vars(DISP_DATE, NEXT_DATE),
         ~excel_numeric_to_date(parse_number(.))
      )

   return(combined)
}

art <- readArtDb(artPath)
arv <- readArvDisp(arvPath)

con        <- connect("ohasis-lw")
ly_clients <- QB$new(con)$from("ohasis_lake.ly_clients")$get()
dbDisconnect(con)


final <- arv %>%
   mutate(
      ARV_REGIMEN   = coalesce(ARV_REGIMEN, REGIMEN),
      ARV_REGIMEN   = case_when(
         ARV_REGIMEN == 'TENOFOVIR+EMTRICITABINE+EFAVIRENZ' ~ 'TDF+FTC+EFV',
         ARV_REGIMEN == 'LAMIVUDINE 300MG / TENOFOVIR DISOPROXIL FUMARATE 300MG + LOPINAVIR 200MG + RITONAVIR 50MG (3TC/TDF + LPV/R) (LAMI/TENO + LOPI/RITO)' ~ 'TDF/3TC+LPV/r',
         ARV_REGIMEN == 'ZIDOVUDINE-LAMIVUDINE-RILPIVIRINE + EFAVIRENZ' ~ 'AZT/3TC+RIL+EFV',
         ARV_REGIMEN == 'LAMIVUDINE 150MG / ZIDOVUDINE 300MG + LOPINAVIR 200MG + RITONAVIR 50MG (3TC/AZT + LPV/R) (LAMI/ZIDO + LOPI/RITO)' ~ 'AZT/3TC+LPV/r',
         ARV_REGIMEN == 'DOLUTEGRAVIR 50MG / LAMIVUDINE 300MG / TENOFOVIR DISOPROXIL FUMARATE 300MG (DTG/3TC/TDF) (TLD) + DTG' ~ 'TDF/3TC/DTG+DTG',
         ARV_REGIMEN == 'ABACAVIR 300MG + LAMIVUDINE 150MG + EFAVIRENZ 600MG (ABC + 3TC + EFV)' ~ 'ABC+3TC+EFV',
         ARV_REGIMEN == 'DOLUTEGRAVIR 50MG / LAMIVUDINE 300MG / TENOFOVIR DISOPROXIL FUMARATE 300MG (DTG/3TC/TDF) (TLD)' ~ 'TDF/3TC/DTG',
         ARV_REGIMEN == 'LAMIVUDINE 300MG +LOPINAVIR /RITONAVIR(RITOCOM) 200MG/50MG+DOLUTEGRAVIR 50MG' ~ '3TC+LPV/r+DTG',
         ARV_REGIMEN == 'DOLUTEGRAVIR 50MG / LAMIVUDINE 300MG / TENOFOVIR DISOPROXIL FUMARATE 300MG (DTG/3TC/TDF) (TLD' ~ 'TDF/3TC/DTG',
         ARV_REGIMEN == 'EFAVIRENZ 600MG + LAMIVUDINE 300MG + TENOFOVIR DISOPROXIL FUMARATE 300MG (EFV/3TC/TDF) (LTE)' ~ 'TDF/3TC/EFV',
         ARV_REGIMEN == 'DOLUTEGRAVIR 50 MG+EMTRICITABINE 200MG+ TENOFOVIR ALAFENAMIDE 25 MG' ~ 'TDF+FTC+DTG',
         ARV_REGIMEN == 'ABACAVIR-LAMIVUDINE-NEVIRAPINE' ~ 'ABC+3TC+NVP',
         ARV_REGIMEN == 'ARV UNKNOWN' ~ '',
         ARV_REGIMEN == 'LAMIVUDINE 150MG / ZIDOVUDINE 300MG + EFAVIRENZ 600MG (3TC/AZT + EFV) (LAMI/ZIDO + EFV)' ~ 'AZT/3TC+EFV',
         ARV_REGIMEN == 'LAMIVUDINE 300MG +LOPINAVIR/RITONAVIR(RITOCOM)+DOLUTEGRAVIR' ~ '3TC+LPV/r+DTG',
         TRUE ~ ARV_REGIMEN
      ),


      CLIENT_TYPE   = case_when(
         CLIENT_TYPE == "COURIER" ~ "8",
         CLIENT_TYPE == "PICK-UP" ~ "2",
         TRUE ~ CLIENT_TYPE
      ),
      TB_SCREEN     = if_else(!is.na(TB_SCREEN), "1", NA_character_),
      VISIT_TYPE    = case_when(
         str_detect(VISIT_TYPE, "CONTINUING") ~ "2",
         str_detect(VISIT_TYPE, "FIRST CONSULT") ~ "1",
         str_detect(VISIT_TYPE, "FOLLOW-UP") ~ "2",
         str_detect(VISIT_TYPE, "SHIFTING") ~ "2",
         str_detect(VISIT_TYPE, "TRANSFER OUT") ~ "2",
         TRUE ~ VISIT_TYPE
      ),
      TX_STATUS     = case_when(
         str_detect(TX_STATUS, "CONTINUING") ~ "2",
         str_detect(TX_STATUS, "ENROLLING") ~ "1",
         TRUE ~ TX_STATUS
      ),
      TB_STATUS     = if_else(!is.na(TB_IPT_STATUS), "0", NA_character_),
      TB_IPT_STATUS = case_when(
         TB_IPT_STATUS == "NOT ON TPT" ~ "0",
         TB_IPT_STATUS == "STARTED" ~ "12",
         TB_IPT_STATUS == "ONGOING" ~ "11",
         TB_IPT_STATUS == "ENDED" ~ "13",
         TRUE ~ TB_IPT_STATUS
      ),
   ) %>%
   left_join(art %>% select(-BRANCH, row_id), join_by(PATIENT_CODE)) %>%
   mutate(
      UIC           = coalesce(UIC.x, UIC.y),
      PHILHEALTH_NO = coalesce(PHILHEALTH_NO.x, PHILHEALTH_NO.y),
   ) %>%
   select(-ends_with(".x"), -ends_with(".y")) %>%
   left_join(
      y  = ly_clients %>%
         filter(!is.na(CENTRAL_ID)) %>%
         rename(PATIENT_CODE = CLIENT_CODE),
      by = join_by(
         BRANCH,
         PATIENT_CODE,
         BIRTHDATE,
         LAST,
         FIRST,
         MIDDLE,
         SUFFIX,
         SEX,
         CLIENT_MOBILE,
         CLIENT_EMAIL,
         UIC,
         PHILHEALTH_NO
      )
   )
#
# new <- final %>%
#    mutate(
#       FIRST = coalesce(FIRST, NAME),
#    ) %>%
#    filter(is.na(CENTRAL_ID)) %>%
#    distinct(
#       CENTRAL_ID,
#       CONFIRMATORY_CODE,
#       PATIENT_CODE,
#       UIC,
#       BIRTHDATE,
#       SEX,
#       FIRST,
#       MIDDLE,
#       LAST,
#       SUFFIX,
#       PHILHEALTH_NO,
#       CLIENT_MOBILE,
#       CLIENT_EMAIL,
#       CURR_ADDR
#    )
#
# write_sheet(new, "1h0j-YG5-Y4seny8QBCB2KRQnWTCBmkvPE-vYDjBH_ig", "new")


##  add patients to ly_clients table

add     <- read_sheet("1h0j-YG5-Y4seny8QBCB2KRQnWTCBmkvPE-vYDjBH_ig", "new")
max_id  <- max(ly_clients$row_id)
created <- final %>%
   mutate(
      FIRST = coalesce(FIRST, NAME),
   ) %>%
   filter(is.na(CENTRAL_ID)) %>%
   select(-CENTRAL_ID) %>%
   mutate(
      row_id = max_id + row_number()
   ) %>%
   left_join(
      y  = add %>% mutate(CONFIRMATORY_CODE = as.character(CONFIRMATORY_CODE)),
      by = join_by(
         CONFIRMATORY_CODE,
         PATIENT_CODE,
         UIC,
         BIRTHDATE,
         SEX,
         FIRST,
         MIDDLE,
         LAST,
         SUFFIX,
         PHILHEALTH_NO,
         CLIENT_MOBILE,
         CLIENT_EMAIL,
         CURR_ADDR
      )
   ) %>%
   select(
      row_id,
      CENTRAL_ID,
      BRANCH,
      PATIENT_CODE,
      BIRTHDATE,
      LAST,
      FIRST,
      MIDDLE,
      SUFFIX,
      SEX,
      CLIENT_MOBILE,
      CLIENT_EMAIL,
      UIC,
      PHILHEALTH_NO
   ) %>%
   mutate(
      FACI_ID    = '130001',
      PATIENT_ID = na_if(CENTRAL_ID, "-")
   )


created %<>%
   filter(!is.na(PATIENT_ID)) %>%
   bind_rows(
      batch_px_ids(created %>% filter(is.na(PATIENT_ID)), PATIENT_ID, FACI_ID, "row_id")
   ) %>%
   select(-FACI_ID, -CENTRAL_ID) %>%
   rename(
      CLIENT_CODE = PATIENT_CODE,
      CENTRAL_ID  = PATIENT_ID,
   )


con <- ohasis$conn("lw")
dbxUpsert(con, Id(schema = "ohasis_lake", table = "ly_clients"), created, "row_id")
dbDisconnect(con)


# main process

#  uploaded --------------------------------------------------------------------

min <- min("2025-01-01")
max <- max("2025-02-28")

db          <- "ohasis_warehouse"
lw_conn     <- ohasis$conn("lw")
# prev_upload <- dbTable(
#    lw_conn,
#    db,
#    "form_art_bc",
#    raw_where = TRUE,
#    where     = glue(r"(
#          (VISIT_DATE BETWEEN '{min}' AND '{max}') AND
#             CREATED_BY = '1300000048'
#    )")
# )
prev_upload <- QB$new(lw_conn)$
   from('ohasis_warehouse.form_art_bc AS art')$
   leftJoin("ohasis_warehouse.id_registry AS id", "art.PATIENT_ID", "=", "id.PATIENT_ID")$
   select("art.REC_ID", "art.VISIT_DATE", "art.MEDICINE_SUMMARY", "art.CREATED_BY", "art.CREATED_AT", "art.PATIENT_ID")$
   selectRaw("COALESCE(id.CENTRAL_ID, art.PATIENT_ID) AS CENTRAL_ID")$
   whereBetween("art.VISIT_DATE", c(min, max))$
   get()
prev_upload %<>%
   mutate_if(
      .predicate = is.POSIXct,
      ~as.Date(.)
   )
dbDisconnect(lw_conn)

##  new data -------------------------------------------------------------------

faci_id <- read_sheet("1c334aEKFTOl3Cg9Uji7tq1rZjlPM8RKpdXwQgOGyAIg", "facility_id")

for_import <- final %>%
   filter(DISP_DATE >= min) %>%
   filter(DISP_DATE <= max) %>%
   left_join(faci_id %>% rename(BRANCH = SITE)) %>%
   mutate(
      DISP_DATE = as.Date(DISP_DATE),
   ) %>%
   rename(
      VISIT_DATE       = DISP_DATE,
      SERVICE_FACI     = FACI_ID,
      SERVICE_SUB_FACI = SUB_FACI_ID,
      CLINIC_NOTES     = REMARKS,
      LATEST_NEXT_DATE = NEXT_DATE,
      MEDICINE_SUMMARY = ARV_REGIMEN,
      PATIENT_ID       = CENTRAL_ID
   ) %>%
   mutate(
      RECORD_DATE  = VISIT_DATE,
      FORM_VERSION = "ART Form (v2021)",
   ) %>%
   # get records id if existing
   left_join(
      y  = prev_upload %>%
         select(
            PATIENT_ID = CENTRAL_ID,
            REC_ID,
            CREATED_BY,
            CREATED_AT,
            CENTRAL_ID,
            VISIT_DATE
         ),
      by = join_by(PATIENT_ID, VISIT_DATE)
   ) %>%
   mutate(BIRTHDATE = as.Date(BIRTHDATE))

TIMESTAMP <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
for_import %<>%
   # retain only not uploaded and those with changes
   filter(!is.na(PATIENT_ID), !is.na(MEDICINE_SUMMARY)) %>%
   anti_join(
      y  = prev_upload,
      # by = join_by(!!!intersect(names(for_import), names(prev_upload))),
      by = join_by(REC_ID, VISIT_DATE, MEDICINE_SUMMARY),
   ) %>%
   mutate(
      old_rec    = if_else(!is.na(REC_ID), 1, 0, 0),
      CREATED_BY = coalesce(CREATED_BY, "1300000048"),
      CREATED_AT = coalesce(as.character(CREATED_AT), TIMESTAMP),
      UPDATED_BY = if_else(old_rec == 1, "1300000048", NA_character_),
      UPDATED_AT = if_else(old_rec == 1, TIMESTAMP, NA_character_)
   ) %>%
   relocate(any_of(names(prev_upload)), .before = 1) %>%
   select(-old_rec) %>%
   distinct(row_id, .keep_all = TRUE)

final_import <- for_import %>%
   filter(!is.na(REC_ID)) %>%
   bind_rows(
      batch_rec_ids(for_import %>% filter(is.na(REC_ID)), REC_ID, CREATED_BY, "row_id")
   )


final_import %<>%
   mutate(
      UPDATED_BY = "1300000048",
      UPDATED_AT = TIMESTAMP
   ) %>%
   left_join(
      y  = prev_upload %>%
         select(REC_ID, CORR_PID = PATIENT_ID),
      by = join_by(REC_ID)
   ) %>%
   mutate(
      PATIENT_ID = coalesce(CORR_PID, PATIENT_ID)
   )
##  table formats
tables           <- list()
tables$px_record <- list(
   name = "px_record",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = final_import %>%
      mutate(
         FACI_ID     = "130001",
         SUB_FACI_ID = NA_character_,
         DISEASE     = "101000",
         MODULE      = 3
      ) %>%
      select(
         REC_ID,
         PATIENT_ID,
         FACI_ID,
         SUB_FACI_ID,
         RECORD_DATE,
         DISEASE,
         MODULE,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      )
)

tables$px_info <- list(
   name = "px_info",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = final_import %>%
      select(
         REC_ID,
         PATIENT_ID,
         CONFIRMATORY_CODE,
         UIC,
         PATIENT_CODE,
         SEX,
         BIRTHDATE,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      ) %>%
      mutate_at(
         .vars = vars(SEX),
         ~keep_code(.)
      )
)

tables$px_name <- list(
   name = "px_name",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = final_import %>%
      select(
         REC_ID,
         PATIENT_ID,
         FIRST,
         MIDDLE,
         LAST,
         SUFFIX,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      )
)

tables$px_contact <- list(
   name = "px_contact",
   pk   = c("REC_ID", "CONTACT_TYPE"),
   data = final_import %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
         CLIENT_MOBILE,
         CLIENT_EMAIL
      ) %>%
      pivot_longer(
         cols      = c(CLIENT_MOBILE, CLIENT_EMAIL),
         names_to  = "CONTACT_TYPE",
         values_to = "CONTACT"
      ) %>%
      mutate(
         CONTACT_TYPE = case_when(
            CONTACT_TYPE == "CLIENT_MOBILE" ~ "1",
            CONTACT_TYPE == "CLIENT_EMAIL" ~ "2",
            TRUE ~ CONTACT_TYPE
         )
      )
)

# tables$px_addr <- list(
#    name = "px_addr",
#    pk   = c("REC_ID", "ADDR_TYPE"),
#    data = final_import %>%
#       select(
#          REC_ID,
#          CREATED_BY,
#          CREATED_AT,
#          UPDATED_BY,
#          UPDATED_AT,
#          CURR_REG,
#          CURR_PROV,
#          CURR_MUNC,
#          CURR_ADDR
#       ) %>%
#       pivot_longer(
#          cols      = c(
#             ends_with("_REG"),
#             ends_with("_PROV"),
#             ends_with("_MUNC"),
#             ends_with("_ADDR")
#          ),
#          names_to  = "ADDR_DATA",
#          values_to = "ADDR_VALUE"
#       ) %>%
#       separate(
#          col  = "ADDR_DATA",
#          into = c("ADDR_TYPE", "PIECE")
#       ) %>%
#       mutate(
#          ADDR_TYPE = case_when(
#             ADDR_TYPE == "CURR" ~ "1",
#             ADDR_TYPE == "PERM" ~ "2",
#             ADDR_TYPE == "BIRTH" ~ "3",
#             ADDR_TYPE == "DEATH" ~ "4",
#             ADDR_TYPE == "SERVICE" ~ "5",
#             TRUE ~ ADDR_TYPE
#          ),
#          PIECE     = case_when(
#             PIECE == "ADDR" ~ "TEXT",
#             TRUE ~ PIECE
#          )
#       ) %>%
#       pivot_wider(
#          id_cols      = c(REC_ID, CREATED_BY, CREATED_AT, UPDATED_BY, UPDATED_AT, ADDR_TYPE),
#          names_from   = PIECE,
#          values_from  = ADDR_VALUE,
#          names_prefix = "NAME_"
#       ) %>%
#       harp_addr_to_id(
#          ohasis$ref_addr,
#          c(
#             ADDR_REG  = "NAME_REG",
#             ADDR_PROV = "NAME_PROV",
#             ADDR_MUNC = "NAME_MUNC"
#          )
#       ) %>%
#       select(
#          REC_ID,
#          starts_with("ADDR_"),
#          ADDR_TEXT = NAME_TEXT,
#          CREATED_BY,
#          CREATED_AT,
#          UPDATED_BY,
#          UPDATED_AT
#       )
# )

tables$px_faci <- list(
   name = "px_faci",
   pk   = c("REC_ID", "SERVICE_TYPE"),
   data = final_import %>%
      mutate(
         SERVICE_TYPE = "101201",
         SERVICE_FACI = as.character(SERVICE_FACI),
      ) %>%
      select(
         REC_ID,
         FACI_ID     = SERVICE_FACI,
         SUB_FACI_ID = SERVICE_SUB_FACI,
         SERVICE_TYPE,
         VISIT_TYPE,
         CLIENT_TYPE,
         TX_STATUS,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      ) %>%
      mutate_at(
         .vars = vars(VISIT_TYPE, CLIENT_TYPE, TX_STATUS),
         ~keep_code(.)
      )
)

tables$px_form <- list(
   name = "px_form",
   pk   = c("REC_ID", "FORM"),
   data = final_import %>%
      mutate(
         FORM    = "ART Form",
         VERSION = "2021"
      ) %>%
      select(
         REC_ID,
         FORM,
         VERSION,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      )
)

tables$px_profile <- list(
   name = "px_profile",
   pk   = "REC_ID",
   data = final_import %>%
      mutate(
         AGE              = calc_age(BIRTHDATE, VISIT_DATE),
         SELF_IDENT_OTHER = if_else(!(SELF_IDENT %in% c("MAN", "WOMAN", "MALE", "FEMALE")), SELF_IDENT, NA_character_),
         SELF_IDENT       = case_when(
            SELF_IDENT == "MALE" ~ "1",
            SELF_IDENT == "MAN" ~ "1",
            SELF_IDENT == "FEMALE" ~ "2",
            SELF_IDENT == "WOMAN" ~ "2",
            !is.na(SELF_IDENT_OTHER) ~ "3",
            TRUE ~ SELF_IDENT
         ),
      ) %>%
      select(
         REC_ID,
         AGE,
         SELF_IDENT,
         SELF_IDENT_OTHER,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      ) %>%
      mutate_at(
         .vars = vars(SELF_IDENT),
         ~keep_code(.)
      )
)

# tables$px_staging <- list(
#    name = "px_staging",
#    pk   = "REC_ID",
#    data = final_import %>%
#       mutate(
#          AGE = calc_age(BIRTHDATE, VISIT_DATE)
#       ) %>%
#       select(
#          REC_ID,
#          WHO_CLASS,
#          CREATED_BY,
#          CREATED_AT,
#          UPDATED_BY,
#          UPDATED_AT,
#       ) %>%
#       mutate_at(
#          .vars = vars(WHO_CLASS),
#          ~keep_code(.)
#       )
# )

# tables$px_labs <- list(
#    name = "px_labs",
#    pk   = c("REC_ID", "LAB_TEST"),
#    data = final_import %>%
#       select(
#          REC_ID,
#          CREATED_BY,
#          CREATED_AT,
#          UPDATED_BY,
#          UPDATED_AT,
#          starts_with("LAB"),
#       ) %>%
#       mutate_at(
#          .vars = vars(contains("_DATE")),
#          ~as.character(.)
#       ) %>%
#       pivot_longer(
#          cols      = starts_with("LAB"),
#          names_to  = "LAB_DATA",
#          values_to = "LAB_VALUE"
#       ) %>%
#       mutate(
#          LAB_TEST = substr(LAB_DATA, 5, stri_locate_last_fixed(LAB_DATA, "_") - 1),
#          PIECE    = substr(LAB_DATA, stri_locate_last_fixed(LAB_DATA, "_") + 1, 1000),
#       ) %>%
#       mutate(
#          LAB_TEST = case_when(
#             LAB_TEST == "HBSAG" ~ "1",
#             LAB_TEST == "CREA" ~ "2",
#             LAB_TEST == "SYPH" ~ "3",
#             LAB_TEST == "VL" ~ "4",
#             LAB_TEST == "CD4" ~ "5",
#             LAB_TEST == "XRAY" ~ "6",
#             LAB_TEST == "XPERT" ~ "7",
#             LAB_TEST == "DSSM" ~ "8",
#             LAB_TEST == "HIVDR" ~ "9",
#             LAB_TEST == "HEMO" ~ "10",
#             LAB_TEST == "HEMOG" ~ "10",
#             TRUE ~ LAB_TEST
#          )
#       ) %>%
#       distinct(REC_ID, CREATED_AT, CREATED_BY, LAB_TEST, PIECE, .keep_all = TRUE) %>%
#       pivot_wider(
#          id_cols      = c(REC_ID, CREATED_AT, CREATED_BY, LAB_TEST),
#          names_from   = PIECE,
#          values_from  = LAB_VALUE,
#          names_prefix = "LAB_"
#       ) %>%
#       filter(!is.na(LAB_DATE) | !is.na(LAB_RESULT)) %>%
#       arrange(REC_ID, LAB_TEST)
# )

tables$px_tb <- list(
   name = "px_tb",
   pk   = "REC_ID",
   data = final_import %>%
      mutate(
         TB_STATUS = case_when(
            !is.na(TB_IPT_STATUS) ~ "0_No active TB",
            TRUE ~ TB_STATUS
         )
      ) %>%
      select(
         REC_ID,
         TB_SCREEN,
         TB_STATUS,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      ) %>%
      mutate_at(
         .vars = vars(TB_SCREEN, TB_STATUS),
         ~keep_code(.)
      )
)

tables$px_tb_ipt <- list(
   name = "px_tb_ipt",
   pk   = "REC_ID",
   data = final_import %>%
      mutate(
         TB_STATUS = case_when(
            !is.na(TB_IPT_STATUS) ~ "0_No active TB",
            TRUE ~ TB_STATUS
         )
      ) %>%
      select(
         REC_ID,
         TB_IPT_STATUS,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      ) %>%
      mutate_at(
         .vars = vars(TB_IPT_STATUS),
         ~keep_code(.)
      )
)

# tables$px_prophylaxis <- list(
#    name = "px_prophylaxis",
#    pk   = c("REC_ID", "PROPHYLAXIS"),
#    data = final_import %>%
#       select(
#          REC_ID,
#          CREATED_BY,
#          CREATED_AT,
#          UPDATED_BY,
#          UPDATED_AT,
#          starts_with("PROPH_"),
#       ) %>%
#       pivot_longer(
#          cols      = starts_with("PROPH_"),
#          names_to  = "PROPHYLAXIS",
#          values_to = "IS_PROPH"
#       ) %>%
#       mutate(
#          PROPHYLAXIS = case_when(
#             PROPHYLAXIS == "PROPH_COTRI" ~ "2",
#             PROPHYLAXIS == "PROPH_AZITHRO" ~ "3",
#             PROPHYLAXIS == "PROPH_FLUCA" ~ "4",
#             TRUE ~ PROPHYLAXIS
#          ),
#          IS_PROPH    = keep_code(IS_PROPH)
#       )
# )

tables$px_medicine <- list(
   name = "px_medicine",
   pk   = c("REC_ID", "MEDICINE", "DISP_NUM"),
   data = final_import %>%
      separate_longer_delim(
         cols  = MEDICINE_SUMMARY,
         delim = "+"
      ) %>%
      mutate(
         SERVICE_FACI = as.character(SERVICE_FACI),
         UNIT_BASIS = "2",
         MEDICINE   = case_when(
            MEDICINE_SUMMARY == "TDF/3TC/DTG" ~ "2029",
            MEDICINE_SUMMARY == "TDF/3TC/EFV" ~ "2015",
            MEDICINE_SUMMARY == "AZT/3TC" ~ "2018",
            MEDICINE_SUMMARY == "LPV/r" ~ "2009",
            MEDICINE_SUMMARY == "DTG" ~ "2035",
            MEDICINE_SUMMARY == "EFV" ~ "2004",
            MEDICINE_SUMMARY == "3TC" ~ "2023",
            MEDICINE_SUMMARY == "ABC" ~ "2002",
            MEDICINE_SUMMARY == "FTC" ~ "2031",
            MEDICINE_SUMMARY == "NVP" ~ "2011",
            MEDICINE_SUMMARY == "RIL" ~ "2014",
            MEDICINE_SUMMARY == "TDF" ~ "2017",
            MEDICINE_SUMMARY == "TDF/3TC" ~ "2016",
         ),
      ) %>%
      filter(!is.na(MEDICINE)) %>%
      group_by(row_id) %>%
      mutate(
         DISP_NUM = row_number(),
         # DISP_TOTAL = TYPICAL_PER_DAY * DISP_TOTAL
      ) %>%
      ungroup() %>%
      select(
         REC_ID,
         FACI_ID     = SERVICE_FACI,
         SUB_FACI_ID = SERVICE_SUB_FACI,
         MEDICINE,
         DISP_NUM,
         UNIT_BASIS,
         PER_DAY,
         DISP_TOTAL,
         MEDICINE_LEFT,
         MEDICINE_MISSED,
         DISP_DATE   = VISIT_DATE,
         NEXT_DATE   = LATEST_NEXT_DATE,
      )
)

db_conn <- ohasis$conn("db")
dbxDelete(
   db_conn,
   Id(schema = "ohasis_interim", table = "px_medicine"),
   final_import %>% select(REC_ID),
   batch_size = 1000
)
lapply(tables, function(ref, db_conn) {
   log_info("Uploading {green(ref$name)}.")
   table_space <- Id(schema = "ohasis_interim", table = ref$name)
   dbxUpsert(db_conn, table_space, ref$data, ref$pk)
   # dbExecute(db_conn, glue("DELETE FROM ohasis_interim.{ref$name} WHERE REC_ID IN (?)"), params = list(unique(ref$data$REC_ID)))
}, db_conn)
dbDisconnect(db_conn)
