dr         <- list()
dr$harp_dx <- read_dta(ohasis$get_data("harp_dx", "2022", "05"))
dr$harp_dx %<>%
   filter(year >= 2016)

dr$dm20210513 <- read_sheet("1LnoeeFukX38-bXxW2Qj89TNKHM5IUFEoldbiWUgTnpI", "DM 2021-0513")
dr$dm20210201 <- read_sheet("1LnoeeFukX38-bXxW2Qj89TNKHM5IUFEoldbiWUgTnpI", "DM 2021-0201")

services        <- c("101101", "101103", "101104", "101105")
db_conn         <- ohasis$conn("db")
# dr$px_faci <- dbReadTable(db_conn, dbplyr::in_schema("ohasis_interim", "px_faci"))
dr$px_faci      <- dbxSelect(db_conn, "SELECT * FROM ohasis_interim.px_faci WHERE SERVICE_TYPE IN (?)", list(services))
dr$service_dx   <- dbxSelect(db_conn, "SELECT * FROM ohasis_interim.facility_service WHERE SERVICE IN (?)", "101102")
dr$service_tx   <- dbxSelect(db_conn, "SELECT * FROM ohasis_interim.facility_service WHERE SERVICE IN (?)", "101201")
dr$service_cfbs <- dbxSelect(db_conn, "SELECT * FROM ohasis_interim.facility_service WHERE SERVICE IN (?)", list(c("101103", "101104")))
dr$service_prep <- dbxSelect(db_conn, "SELECT * FROM ohasis_interim.facility_service WHERE SERVICE IN (?)", "101301")
dbDisconnect(db_conn)

##  dx facilities w/in the past 5 years ----------------------------------------

dr$faci_dx <- dr$harp_dx %>%
   left_join(
      y  = dr$px_faci %>%
         select(REC_ID, FACI_ID, SUB_FACI_ID),
      by = 'REC_ID'
   ) %>%
   distinct(FACI_ID, SUB_FACI_ID) %>%
   mutate(
      SUB_FACI_ID = if_else(
         condition = is.na(SUB_FACI_ID),
         true      = "",
         false     = SUB_FACI_ID
      )
   ) %>%
   left_join(
      y  = ohasis$ref_faci %>%
         mutate(
            LAT  = format(LAT, digits = 8),
            LONG = format(LONG, digits = 8),
         ) %>%
         select(FACI_ID, SUB_FACI_ID, LONG, LAT, FACI_ADDR, starts_with("FACI_NAME"), FACI_PSGC_MUNC),
      by = c("FACI_ID", "SUB_FACI_ID")
   ) %>%
   filter(!is.na(FACI_NAME)) %>%
   mutate(
      ORDER = if_else(SUB_FACI_ID == "", 1, 9999)
   ) %>%
   arrange(FACI_PSGC_MUNC, FACI_ID, ORDER, SUB_FACI_ID) %>%
   distinct(FACI_NAME, LONG, LAT, FACI_NAME_REG, FACI_NAME_PROV, FACI_NAME_MUNC, FACI_ADDR, .keep_all = TRUE) %>%
   arrange(FACI_PSGC_MUNC, ORDER, FACI_NAME) %>%
   group_by(FACI_NAME_CLEAN) %>%
   mutate(
      grp_id = if_else(!is.na(FACI_NAME_CLEAN), row_number(), as.integer(1))
   ) %>%
   ungroup() %>%
   filter(grp_id == 1) %>%
   select(
      `Health Reporting Unit` = FACI_NAME,
      `Longitude`             = LONG,
      `Latitude`              = LAT,
      `Address`               = FACI_ADDR,
      `Region`                = FACI_NAME_REG,
      `Province`              = FACI_NAME_PROV,
      `City/Municipality`     = FACI_NAME_MUNC
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(.) %>% if_else(. == "", NA_character_, .)
   )

##  crcl facilities ------------------------------------------------------------

dr$faci_crcl <- ohasis$ref_faci %>%
   mutate(
      LAT  = format(LAT, digits = 8),
      LONG = format(LONG, digits = 8),
   ) %>%
   select(FACI_ID, SUB_FACI_ID, LONG, LAT, FACI_ADDR, starts_with("FACI_NAME"), FACI_PSGC_MUNC, FACI_CODE) %>%
   inner_join(
      y  = dr$service_dx %>% select(FACI_ID),
      by = "FACI_ID"
   ) %>%
   mutate(
      IS_HUB = if_else(nchar(FACI_CODE) == 3, 1, 0),
      ORDER  = if_else(SUB_FACI_ID == "", 1, 9999)
   ) %>%
   arrange(FACI_PSGC_MUNC, FACI_ID, ORDER, SUB_FACI_ID) %>%
   distinct(FACI_NAME, LONG, LAT, FACI_NAME_REG, FACI_NAME_PROV, FACI_NAME_MUNC, FACI_ADDR, .keep_all = TRUE) %>%
   arrange(FACI_PSGC_MUNC, ORDER, FACI_NAME) %>%
   group_by(FACI_NAME_CLEAN) %>%
   mutate(
      grp_id = if_else(!is.na(FACI_NAME_CLEAN), row_number(), as.integer(1))
   ) %>%
   ungroup() %>%
   filter(grp_id == 1) %>%
   left_join(
      y  = dr$dm20210513 %>%
         filter(CLASS != "Other") %>%
         select(FACI_ID) %>%
         mutate(Issuance = "DM 2021-0513"),
      by = "FACI_ID"
   ) %>%
   bind_rows(
      ohasis$ref_faci %>%
         filter(FACI_NAME == "SLH SACCL") %>%
         mutate(
            LAT  = format(LAT, digits = 8),
            LONG = format(LONG, digits = 8),
         )
   ) %>%
   mutate(
      drop           = case_when(
         stri_detect_fixed(FACI_NAME, "LoveY") & is.na(Issuance) ~ 1,
         stri_detect_fixed(FACI_NAME, "Batasan Social Hygiene Clinic") ~ 1,
         stri_detect_fixed(FACI_NAME, "Klinika Batasan") ~ 1,
         stri_detect_fixed(FACI_NAME, "Pasig Clinical Laboratory") ~ 1,
         TRUE ~ 0
      ),
      Issuance       = case_when(
         FACI_NAME == "San Lazaro Hospital (SLH)" ~ NA_character_,
         TRUE ~ Issuance
      ),
      Classification = case_when(
         FACI_NAME == "SLH SACCL" ~ "NRL",
         TRUE ~ "CrCL"
      ),
      FACI_NAME      = case_when(
         FACI_NAME == "San Lazaro Hospital (SLH)" ~ "San Lazaro Hospital (SLH) Central Laboratory",
         FACI_NAME == "SLH SACCL" ~ "San Lazaro Hospital (SLH) NRL-SACCL",
         TRUE ~ FACI_NAME
      )
   ) %>%
   filter(drop == 0) %>%
   select(
      `Health Reporting Unit` = FACI_NAME,
      `Longitude`             = LONG,
      `Latitude`              = LAT,
      `Address`               = FACI_ADDR,
      `Region`                = FACI_NAME_REG,
      `Province`              = FACI_NAME_PROV,
      `City/Municipality`     = FACI_NAME_MUNC,
      `Issuance`,
      `Classification`
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(.) %>% if_else(. == "", NA_character_, .)
   ) %>%
   distinct_all()

##  tx facilities --------------------------------------------------------------

dr$faci_tx <- ohasis$ref_faci %>%
   mutate(
      LAT  = format(LAT, digits = 8),
      LONG = format(LONG, digits = 8),
   ) %>%
   select(FACI_ID, SUB_FACI_ID, LONG, LAT, FACI_ADDR, starts_with("FACI_NAME"), FACI_PSGC_MUNC, FACI_CODE) %>%
   inner_join(
      y  = dr$service_tx %>% select(FACI_ID),
      by = "FACI_ID"
   ) %>%
   mutate(
      IS_HUB = if_else(nchar(FACI_CODE) == 3, 1, 0),
      ORDER  = if_else(SUB_FACI_ID == "", 1, 9999)
   ) %>%
   arrange(FACI_PSGC_MUNC, FACI_ID, ORDER, SUB_FACI_ID) %>%
   distinct(FACI_NAME, LONG, LAT, FACI_NAME_REG, FACI_NAME_PROV, FACI_NAME_MUNC, FACI_ADDR, .keep_all = TRUE) %>%
   arrange(FACI_PSGC_MUNC, ORDER, FACI_NAME) %>%
   group_by(FACI_NAME_CLEAN) %>%
   mutate(
      grp_id = if_else(!is.na(FACI_NAME_CLEAN), row_number(), as.integer(1))
   ) %>%
   ungroup() %>%
   filter(grp_id == 1) %>%
   left_join(
      y  = dr$dm20210201 %>%
         filter(CLASS != "Other") %>%
         select(FACI_ID) %>%
         mutate(Issuance = "DM 2021-0201"),
      by = "FACI_ID"
   ) %>%
   select(
      `Health Reporting Unit` = FACI_NAME,
      `Longitude`             = LONG,
      `Latitude`              = LAT,
      `Address`               = FACI_ADDR,
      `Region`                = FACI_NAME_REG,
      `Province`              = FACI_NAME_PROV,
      `City/Municipality`     = FACI_NAME_MUNC,
      `Issuance`
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(.) %>% if_else(. == "", NA_character_, .)
   ) %>%
   filter(
      !(`Health Reporting Unit` %in% c("SAIL-Trece Martires", "Valenzuela Medical Center (Clinical Laboratory)"))
   )

##  prep facilities ------------------------------------------------------------

dr$faci_prep <- ohasis$ref_faci %>%
   mutate(
      LAT  = format(LAT, digits = 8),
      LONG = format(LONG, digits = 8),
   ) %>%
   select(FACI_ID, SUB_FACI_ID, LONG, LAT, FACI_ADDR, starts_with("FACI_NAME"), FACI_PSGC_MUNC, FACI_CODE) %>%
   inner_join(
      y  = dr$service_prep %>% select(FACI_ID),
      by = "FACI_ID"
   ) %>%
   mutate(
      IS_HUB = if_else(nchar(FACI_CODE) == 3, 1, 0),
      ORDER  = if_else(SUB_FACI_ID == "", 1, 9999)
   ) %>%
   arrange(FACI_PSGC_MUNC, FACI_ID, ORDER, SUB_FACI_ID) %>%
   distinct(FACI_NAME, LONG, LAT, FACI_NAME_REG, FACI_NAME_PROV, FACI_NAME_MUNC, FACI_ADDR, .keep_all = TRUE) %>%
   arrange(FACI_PSGC_MUNC, ORDER, FACI_NAME) %>%
   group_by(FACI_NAME_CLEAN) %>%
   mutate(
      grp_id = if_else(!is.na(FACI_NAME_CLEAN), row_number(), as.integer(1))
   ) %>%
   ungroup() %>%
   filter(grp_id == 1) %>%
   select(
      `Health Reporting Unit` = FACI_NAME,
      `Longitude`             = LONG,
      `Latitude`              = LAT,
      `Address`               = FACI_ADDR,
      `Region`                = FACI_NAME_REG,
      `Province`              = FACI_NAME_PROV,
      `City/Municipality`     = FACI_NAME_MUNC
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(.) %>% if_else(. == "", NA_character_, .)
   ) %>%
   filter(`Latitude` != "NA")

##  cfbs facilities ------------------------------------------------------------

dr$faci_cfbs <- ohasis$ref_faci %>%
   mutate(
      LAT  = format(LAT, digits = 8),
      LONG = format(LONG, digits = 8),
   ) %>%
   select(FACI_ID, SUB_FACI_ID, LONG, LAT, FACI_ADDR, starts_with("FACI_NAME"), FACI_PSGC_MUNC, FACI_CODE) %>%
   inner_join(
      y  = dr$service_cfbs %>% select(FACI_ID),
      by = "FACI_ID"
   ) %>%
   mutate(
      IS_HUB = if_else(nchar(FACI_CODE) == 3, 1, 0),
      ORDER  = if_else(SUB_FACI_ID == "", 1, 9999)
   ) %>%
   arrange(FACI_PSGC_MUNC, FACI_ID, ORDER, SUB_FACI_ID) %>%
   distinct(FACI_NAME, LONG, LAT, FACI_NAME_REG, FACI_NAME_PROV, FACI_NAME_MUNC, FACI_ADDR, .keep_all = TRUE) %>%
   arrange(FACI_PSGC_MUNC, ORDER, FACI_NAME) %>%
   group_by(FACI_NAME_CLEAN) %>%
   mutate(
      grp_id = if_else(!is.na(FACI_NAME_CLEAN), row_number(), as.integer(1))
   ) %>%
   ungroup() %>%
   filter(grp_id == 1) %>%
   select(
      `Health Reporting Unit` = FACI_NAME,
      `Longitude`             = LONG,
      `Latitude`              = LAT,
      `Address`               = FACI_ADDR,
      `Region`                = FACI_NAME_REG,
      `Province`              = FACI_NAME_PROV,
      `City/Municipality`     = FACI_NAME_MUNC
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(.) %>% if_else(. == "", NA_character_, .)
   ) %>%
   filter(`Latitude` != "NA")

##  format excel file ----------------------------------------------------------

p_load(openxlsx)
dr$wb    <- createWorkbook()
dr$hs    <- createStyle(
   fontName       = "Calibri",
   fontSize       = 10,
   halign         = "center",
   valign         = "center",
   textDecoration = "bold",
   fgFill         = "#ffe699"
)
dr$cs    <- createStyle(
   fontName = "Calibri",
   fontSize = 10,
   numFmt   = openxlsx_getOp("numFmt", "TEXT")
)
dr$coord <- createStyle(
   fontName = "Calibri",
   fontSize = 10,
   numFmt   = openxlsx_getOp("numFmt", "0.00000000")
)

## Sheet 1
addWorksheet(dr$wb, "rHIVda")
writeData(dr$wb, sheet = 1, x = dr$faci_crcl)
addWorksheet(dr$wb, "HIV Testing (MedTech)")
writeData(dr$wb, sheet = 2, x = dr$faci_dx)
addWorksheet(dr$wb, "ART")
writeData(dr$wb, sheet = 3, x = dr$faci_tx)
addWorksheet(dr$wb, "PreP")
writeData(dr$wb, sheet = 4, x = dr$faci_prep)
addWorksheet(dr$wb, "CFBS")
writeData(dr$wb, sheet = 5, x = dr$faci_cfbs)

data      <- list()
data[[1]] <- dr$faci_crcl
data[[2]] <- dr$faci_dx
data[[3]] <- dr$faci_tx
data[[4]] <- dr$faci_prep
data[[5]] <- dr$faci_cfbs


write_dta(data[[1]] %>%
             rename_all(~stri_replace_all_fixed(., " ", "") %>%
                stri_replace_all_fixed(., "/", "")), "H:/Data Requests/20220525-doc_maffy/faci_crcl.dta")
write_dta(data[[2]] %>%
             rename_all(~stri_replace_all_fixed(., " ", "") %>%
                stri_replace_all_fixed(., "/", "")), "H:/Data Requests/20220525-doc_maffy/faci_dx.dta")
write_dta(data[[3]] %>%
             rename_all(~stri_replace_all_fixed(., " ", "") %>%
                stri_replace_all_fixed(., "/", "")), "H:/Data Requests/20220525-doc_maffy/faci_tx.dta")
write_dta(data[[4]] %>%
             rename_all(~stri_replace_all_fixed(., " ", "") %>%
                stri_replace_all_fixed(., "/", "")), "H:/Data Requests/20220525-doc_maffy/faci_prep.dta")
write_dta(data[[5]] %>%
             rename_all(~stri_replace_all_fixed(., " ", "") %>%
                stri_replace_all_fixed(., "/", "")), "H:/Data Requests/20220525-doc_maffy/faci_cfbs.dta")

for (file in list.files("H:/Data Requests/20220704-doc_maffy", "*.dta", full.names = TRUE)) {
   # initialize empty stata commands
   stataCMD <- ""

   # use file
   stataCMD <- glue(r"(u "{file}", clear)")
   # format and save file
   stataCMD <- glue(paste0(stataCMD, "\n", r"(
ds, has(type string)
foreach var in `r(varlist)' {{
   loc type : type `var'
   loc len = substr("`type'", 4, 1000)

   cap form `var' %-`len's
}}

destring Longitude, replace
destring Latitude, replace
//form *date* %tdCCYY-NN-DD
compress
sa "{file}", replace
   )"))

   # run command
   stata(stataCMD)
}

for (i in 1:5) {
   addStyle(dr$wb, sheet = i, dr$hs, rows = 1, cols = seq_len(ncol(data[[i]])), gridExpand = TRUE)
   addStyle(dr$wb, sheet = i, dr$cs, rows = 2:(nrow(data[[i]]) + 1), cols = seq_len(ncol(data[[i]])), gridExpand = TRUE)
   addStyle(dr$wb, sheet = i, dr$coord, rows = 2:(nrow(data[[i]]) + 1), cols = 2:3, gridExpand = TRUE)
   setColWidths(dr$wb, i, cols = seq_len(ncol(data[[i]])), widths = 'auto')
   setRowHeights(dr$wb, i, rows = seq_len(nrow(data[[i]]) + 1), heights = 14)
   freezePane(dr$wb, i, firstRow = TRUE)

   data[[i]] %<>%
      mutate(
         Longitude = as.numeric(Longitude),
         Latitude  = as.numeric(Latitude),
      )
}
saveWorkbook(dr$wb, "H:/Data Requests/20220704-doc_maffy/health_units.xlsx", overwrite = TRUE)
saveRDS(data, "H:/Data Requests/20220704-doc_maffy/health_units.RDS")
saveWorkbook(dr$wb, "H:/20220704_health_units.xlsx", overwrite = TRUE)
