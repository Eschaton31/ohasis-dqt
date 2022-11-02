p_load(arrow)
epictr$data$hiv_test <- read_parquet(file.path(Sys.getenv("LABBS"), "20221021014400_labbs_2021-12.parquet")) %>%
   mutate_at(
      .vars = vars(labbs_reg, labbs_prov, labbs_munc),
      ~str_squish(toupper(.))
   ) %>%
   mutate(
      labbs_reg  = case_when(
         labbs_reg == "BARMM" ~ "ARMM",
         labbs_prov == "ALBAY" & labbs_faci == "Oas Rural Health Unit" ~ "5",
         labbs_prov == "LANAO DEL SUR" & labbs_faci == "AMAI PAKPAK MEDICAL CENTER (LABORATORY)" ~ "ARMM",
         labbs_prov == "MAGUINDANAO" & labbs_faci == "COTABATO SANITARIUM" ~ "ARMM",
         labbs_prov == "MAGUINDANAO" & labbs_faci == "COTABATO REGIONAL AND MEDICAL CENTER" ~ "ARMM",
         labbs_reg == "9" & labbs_faci == "ZAMBOANGA CITY MEDICAL CENTER" ~ "9",
         TRUE ~ labbs_reg
      ),
      labbs_prov = case_when(
         labbs_prov == "METRO MANILA" ~ "NCR",
         labbs_prov == "MT. PROVINCE" ~ "MOUNTAIN PROVINCE",
         labbs_prov == "NORTH COTABATO" ~ "COTABATO",
         labbs_prov == "COMPOSTELA VALLEY" ~ "DAVAO DE ORO",
         labbs_prov == "BATAAN" & labbs_munc == "PALAYAN" ~ "NUEVA ECIJA",
         labbs_prov == "BASILAN" & labbs_munc == "BASILAN-RO9" ~ "NUEVA ECIJA",
         labbs_prov == "CAPIZ" & labbs_faci == "Pototan Ruiral Health Unit" ~ "ILOILO",
         labbs_prov == "ILOILO PROVINCE" & labbs_faci == "Dr. Catalino G. Nava Provincial Hospital" ~ "GUIMARAS",
         labbs_prov == "ILOILO" & labbs_faci == "YGEIA MEDICAL CENTER, INC BACOLOD" ~ "NEGROS OCCIDENTAL",
         labbs_prov == "ILOILO CITY" & labbs_faci == "Medicus Diagnostic Center Main Gen Luna" ~ "ILOILO",
         labbs_prov == "ILOILO PROVINCE" & labbs_faci == "Dueñas Rural Health Unit" ~ "ILOILO",
         labbs_prov == "CALBAYOG, SAMAR" & labbs_faci == "Calbayog Rhu" ~ "SAMAR",
         labbs_prov == "SAMAR" & labbs_faci == "Sogod RHU" ~ "SOUTHERN LEYTE",
         labbs_prov == "BASILAN" & labbs_faci == "Basilan General Hospital" ~ "BASILAN-RO9",
         labbs_prov == "BASILAN" & labbs_faci == "Social Hygiene Clinic- Isabela" ~ "BASILAN-RO9",
         labbs_prov == "ZS" & labbs_faci == "Zamboanga Sibugay Provincial Hospital" ~ "ZAMBOANGA DEL SUR",
         labbs_prov == "ZDN" & labbs_faci == "Clinika Dapitan Diagnostic Center" ~ "ZAMBOANGA DEL NORTE",
         labbs_prov == "MISAMIS ORIENTAL" & labbs_faci == "MEDINA GENERAL HOSPITAL" ~ "MISAMIS OCCIDENTAL",
         labbs_prov == "MISAMIS OCCIDENTAL" & labbs_faci == "MAYOR HILARION A. RAMIRO SR. MEDICAL CENTER (LABORATORY)" ~ "MISAMIS OCCIDENTAL",
         labbs_prov == "TAWI-TAWI" & labbs_faci == "ZAMBOANGA CITY MEDICAL CENTER" ~ "ZAMBOANGA DEL SUR",
         labbs_prov == "BAGUIO" & labbs_faci == "Lab Life Specialty Clinic" ~ "BENGUET",
         labbs_prov == "BAGUIO" & labbs_faci == "QA Diagnostic Laboratory" ~ "BENGUET",
         labbs_prov == "AGUSAN DEL SUR" & labbs_faci == "SAN FRANCISCO DIAGNOSTIC & MEDICAL CLINIC & DRUG TESTING CENTER - SAN FRANCISCO" ~ "SURIGAO DEL SUR",
         labbs_prov == "BILIRAN" & labbs_faci == "MD+ Clinic and Diagnostic Center - Alabang" ~ "NCR",
         TRUE ~ labbs_prov
      ),
      labbs_munc = stri_replace_all_fixed(labbs_munc, "STA ", "SANTA "),
      labbs_munc = stri_replace_all_fixed(labbs_munc, "STA.", "SANTA"),
      labbs_munc = case_when(
         stri_detect_fixed(labbs_munc, "MANILA") ~ "MANILA",
         labbs_reg == "NCR" & stri_detect_fixed(labbs_munc, "PASIG") ~ "PASIG",
         labbs_reg == "NCR" & stri_detect_fixed(labbs_munc, "MUNTINLUPA") ~ "MUNTINLUPA",
         labbs_reg == "NCR" & stri_detect_fixed(labbs_munc, "QUEZON") ~ "QUEZON",
         labbs_reg == "NCR" & stri_detect_fixed(labbs_munc, "SAN JUAN") ~ "SAN JUAN",
         labbs_reg == "NCR" & stri_detect_fixed(labbs_munc, "CALOOCAN") ~ "CALOOCAN",
         labbs_prov == "BULACAN" & labbs_munc == "DRT" ~ "DOÑA REMEDIOS TRINIDAD",
         labbs_prov == "BULACAN" & labbs_munc == "DOÑA REMEDIOS TRINIDAD" ~ "DOÑA REMEDIOS TRINIDAD",
         labbs_prov == "BULACAN" & labbs_munc == "BULAKAN" ~ "BULACAN",
         labbs_prov == "BULACAN" & labbs_munc == "BALIWAG" ~ "BALIUAG",
         labbs_prov == "PAMPANGA" & labbs_munc == "CLARK" ~ "ANGELES",
         labbs_prov == "PAMPANGA" & labbs_munc == "FLORIDA BLANCA" ~ "FLORIDABLANCA",
         labbs_prov == "AURORA" & labbs_munc == "AURORA" ~ "MARIA AURORA",
         labbs_prov == "AURORA" & labbs_faci == "Maria Aurora Municipal Health Office" ~ "MARIA AURORA",
         labbs_prov == "AURORA" & labbs_faci == "San Luis Municipal Health Office" ~ "SAN LUIS",
         labbs_prov == "BULACAN" & labbs_faci == "San Ildefonso Municipal Health Office" ~ "SAN ILDEFONSO",
         labbs_prov == "BULACAN" & labbs_faci == "Sta. Maria Municipal Health Office" ~ "SANTA MARIA",
         labbs_prov == "NUEVA ECIJA" & labbs_faci == "Gen. Natividad Municipal Health Office" ~ "GENERAL MAMERTO NATIVIDAD",
         labbs_prov == "NUEVA ECIJA" & labbs_faci == "Gen. Tinio Municipal Health Office" ~ "GENERAL TINIO",
         labbs_prov == "NUEVA ECIJA" & labbs_faci == "Penaranda Municipal Health Office" ~ "PEÑARANDA",
         labbs_prov == "NUEVA ECIJA" & labbs_faci == "San Antonio Municipal Health Office" ~ "SAN ANTONIO",
         labbs_prov == "NUEVA ECIJA" & labbs_faci == "San Isidro Municipal Health Office" ~ "SAN ISIDRO",
         labbs_prov == "NUEVA ECIJA" & labbs_faci == "Sta. Rosa Municipal Health Office" ~ "SANTA ROSA",
         labbs_prov == "NUEVA ECIJA" & labbs_faci == "Sto. Domingo Municipal Health Office" ~ "SANTO DOMINGO",
         labbs_prov == "PAMPANGA" & labbs_faci == "Lakan Community Center" ~ "MABALACAT",
         labbs_prov == "PAMPANGA" & labbs_faci == "San Luis Municipal Health Office" ~ "SAN LUIS",
         labbs_prov == "PAMPANGA" & labbs_faci == "Santo Tomas Municipal Health Office" ~ "SANTO TOMAS",
         labbs_prov == "PAMPANGA" & labbs_faci == "Sta. Ana Municipal Health Office" ~ "SANTA ANA",
         labbs_prov == "PAMPANGA" & labbs_faci == "Sta. Rita Municipal Health Office" ~ "SANTA RITA",
         labbs_prov == "TARLAC" & labbs_faci == "San Clemente Municipal Health Office" ~ "SAN CLEMENTE",
         labbs_prov == "ZAMBALES" & labbs_faci == "San Narciso" ~ "SAN NARCISO",
         labbs_prov == "BULACAN" & labbs_faci == "Guiguinto Municipal Health Office" ~ "GUIGUINTO",
         labbs_prov == "ANTIQUE" & labbs_faci == "San Jose De Buenavista Municipal Health Center" ~ "SAN JOSE",
         labbs_prov == "GUIMARAS" & labbs_faci == "Guimaras Social Hygiene Clinic" ~ "JORDAN",
         labbs_prov == "ILOILO CITY" & labbs_faci == "Medicus Diagnostic Center Main Gen Luna" ~ "ILOILO",
         labbs_prov == "ILOILO PROVINCE" & labbs_faci == "Dueñas Rural Health Unit" ~ "DUEÑAS",
         labbs_prov == "CEBU" & labbs_faci == "SOUTHWESTERN UNIVERSITY MEDICAL CENTER" ~ "CEBU",
         labbs_prov == "LEYTE" & labbs_faci == "Visayas State University Hospital" ~ "BAYBAY",
         labbs_prov == "ZAMBOANGA DEL SUR" & labbs_faci == "Margosatubig Regional Hospital" ~ "MARGOSATUBIG",
         labbs_prov == "MISAMIS OCCIDENTAL" & labbs_faci == "CITY HEALTH OFFICE-OZAMIS" ~ "OZAMIZ",
         labbs_prov == "LANAO DEL NORTE" & labbs_faci == "E & R HOSPITAL AND PHARMACY" ~ "ILIGAN",
         labbs_prov == "LANAO DEL NORTE" & labbs_faci == "DR. UY HOSPITAL" ~ "ILIGAN",
         labbs_prov == "MISAMIS OCCIDENTAL" & labbs_faci == "MISAMIS UNIVERSITY MEDICAL CENTER" ~ "OZAMIZ",
         labbs_prov == "MISAMIS ORIENTAL" & labbs_faci == "MARIA REYNA XAVIER UNIVERSITY HOSPITAL" ~ "CAGAYAN DE ORO",
         labbs_prov == "MISAMIS ORIENTAL" & labbs_faci == "CITY HEALTH OFFICE-GINGOOG" ~ "GINGOOG",
         labbs_prov == "MISAMIS ORIENTAL" & labbs_faci == "MEDINA GENERAL HOSPITAL" ~ "OZAMIZ",
         labbs_prov == "MISAMIS OCCIDENTAL" & labbs_faci == "MAYOR HILARION A. RAMIRO SR. MEDICAL CENTER (LABORATORY)" ~ "OZAMIZ",
         labbs_prov == "SOUTH COTABATO" & labbs_faci == "Sto. Niño - Rural Health Unit" ~ "SANTO NIÑO",
         labbs_prov == "MAGUINDANAO" & labbs_faci == "COTABATO SANITARIUM" ~ "SULTAN KUDARAT",
         labbs_prov == "SOUTH COTABATO" & labbs_faci == "APOPONG Rural Health Unit" ~ "GENERAL SANTOS",
         labbs_prov == "SOUTH COTABATO" & labbs_faci == "BULA Rural Health Unit" ~ "GENERAL SANTOS",
         labbs_prov == "SOUTH COTABATO" & labbs_faci == "CALUMPANG Rural Health Unit" ~ "GENERAL SANTOS",
         labbs_prov == "SOUTH COTABATO" & labbs_faci == "LABANGAL Rural Health Unit" ~ "GENERAL SANTOS",
         labbs_prov == "LAGUNA" & labbs_faci == "NEW SINAI MDI HOSPITAL" ~ "SANTA ROSA",
         labbs_prov == "RIZAL" & labbs_faci == "JALA-JALA RHU" ~ "JALA-JALA",
         labbs_prov == "MAGUINDANAO" & labbs_faci == "DATU BLAH SINSUAT HOSPITAL" ~ "DATU ODIN SINSUAT",
         labbs_prov == "MAGUINADANAO" & labbs_faci == "MAGUINDANAO PROVINCIAL HOSPITAL" ~ "AMPATUAN",
         labbs_prov == "SULU" & labbs_faci == "SULU PROVINCIAL HOSPITAL" ~ "JOLO",
         labbs_prov == "APAYAO" & labbs_faci == "Apayao District Hospital" ~ "CALANASAN",
         labbs_prov == "NCR" & labbs_faci == "St. Rita Medical and Diagnostic Center" ~ "MAKATI",
         TRUE ~ labbs_munc
      ),
      labbs_munc = str_conv(labbs_munc, "UTF-8"),
      hub_type   = toupper(hub_type),
      hub_type   = case_when(
         faci_type == 9 ~ "SHC",
         hub_type == "Tx Hub" ~ "Tx Hub",
         TRUE ~ "Others"
      )
   ) %>%
   left_join(
      y = epictr$ref_addr %>%
         select(
            PSGC_REG,
            PSGC_PROV,
            PSGC_MUNC,
            labbs_reg  = NHSSS_REG,
            labbs_prov = NHSSS_PROV,
            labbs_munc = NHSSS_MUNC
         )
   ) %>%
   # filter(is.na(PSGC_REG)) %>%
   distinct(labbs_faci, labbs_reg, labbs_prov, labbs_munc, .keep_all = TRUE)


lw_conn     <- ohasis$conn("lw")
table_space <- Id(schema = "harp", table = "epictr_linelist_tx")
if (dbExistsTable(lw_conn, table_space))
   dbRemoveTable(lw_conn, table_space)

dbCreateTable(lw_conn, table_space, epictr$data$txreg)
dbExecute(
   lw_conn,
   r"(
   alter table harp.epictr_linelist_tx modify row_id varchar(355) not null;
)"
)

ohasis$upsert(lw_conn, "harp", "epictr_linelist_tx", epictr$data$txreg, c("PSGC_REG", "PSGC_PROV", "PSGC_AEM", "report_yr", "row_id"))

dbDisconnect(lw_conn)

