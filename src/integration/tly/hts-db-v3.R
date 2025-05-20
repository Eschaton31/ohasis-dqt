LyHts <- R6Class(
   "LyHts",
   public = list(
      root          = "",
      months        = "",
      data          = list(
         raw      = tibble(),
         logsheet = tibble()
      ),

      initialize    = function(month = NULL) {
         self$root <- file.path(getwd(), "data", "ly-imports", format(Sys.time(), "%Y%m%d"))

         if (missing(month)) {
            month <- month(Sys.time())
         }

         self$months <- toupper(month.name[seq_len(month)])

         invisible(self)
      },
      download      = function() {
         local_drive_quiet()
         local_gs4_quiet()

         ss     <- "1smORFFrPwFFrbXQuUUqxNnxyD9VInEPFL7XgL-dmvUM"
         sheets <- range_speedread(ss, "hts", col_types = cols(.default = "c"))

         dir <- file.path(self$root, "hts")
         check_dir(dir)

         for (i in seq_len(nrow(sheets))) {
            branch <- sheets[i,]$branch
            link   <- sheets[i,]$link
            file   <- file.path(dir, stri_c(branch, ".ods"))
            log_info("Downloading = {green(branch)}.")
            drive_download(link, file, overwrite = TRUE)
         }

         invisible(self)
      },
      readAll       = function() {
         files       <- list.files(file.path(self$root, "hts"), full.names = TRUE)
         data        <- lapply(files, self$readFile)
         names(data) <- tools::file_path_sans_ext(basename(files))

         self$data$raw <- bind_rows(data, .id = "Branch") %>%
            mutate(row_id = row_number())

         invisible(self)
      },
      toLogsheet    = function() {
         self$data$logsheet <- self$data$raw %>%
            mutate(
               ENCODEBY            = ASSIGNEDCOUNSELOR,
               UIC_MOM             = if_else(str_length(`UNIQUEIDENTIFIERCODE(UIC)`) == 14, str_mid(`UNIQUEIDENTIFIERCODE(UIC)`, 1, 2), NA_character_),
               UIC_DAD             = if_else(str_length(`UNIQUEIDENTIFIERCODE(UIC)`) == 14, str_mid(`UNIQUEIDENTIFIERCODE(UIC)`, 3, 2), NA_character_),
               UIC_ORDER           = if_else(str_length(`UNIQUEIDENTIFIERCODE(UIC)`) == 14, str_mid(`UNIQUEIDENTIFIERCODE(UIC)`, 5, 2), NA_character_),
               UIC_MO              = if_else(str_length(`UNIQUEIDENTIFIERCODE(UIC)`) == 14, str_mid(`UNIQUEIDENTIFIERCODE(UIC)`, 7, 2), NA_character_),
               UIC_DY              = if_else(str_length(`UNIQUEIDENTIFIERCODE(UIC)`) == 14, str_mid(`UNIQUEIDENTIFIERCODE(UIC)`, 9, 2), NA_character_),
               UIC_YR              = if_else(str_length(`UNIQUEIDENTIFIERCODE(UIC)`) == 14, str_mid(`UNIQUEIDENTIFIERCODE(UIC)`, 11, 4), NA_character_),

               GENDERIDENTITY      = str_replace_all(GENDERIDENTITY, "^I am a ", ""),
               GENDERIDENTITY      = str_replace_all(GENDERIDENTITY, "^I am ", ""),
               GENDERIDENTITYOTHER = if_else(!(GENDERIDENTITY %in% c("MALE", "MAN", "man", "FEMALE", "WOMAN", "woman")), GENDERIDENTITY, NA_character_),
               GENDERIDENTITY      = case_when(
                  GENDERIDENTITY %in% c("MALE", "MAN", "man", "FEMALE", "WOMAN", "woman") ~ toupper(GENDERIDENTITY),
                  !is.na(GENDERIDENTITYOTHER) ~ "OTHERS",
                  TRUE ~ GENDERIDENTITYOTHER
               ),

               CURR_REG            = NA_character_,
               CURR_ADDR           = `CITY/MUNICIPALITYOFRESIDENCE(CURRENT)`,

               PERM_REG            = NA_character_,
               PERM_ADDR           = `CITY/MUNICIPALITYOFRESIDENCE(PERMANENT)`,

               SEXMALE             = case_when(
                  !is.na(`DATEOFLASTSEX(MALE)`) ~ "YES",
                  !is.na(`DATEOFLASTUNPROTECTEDSEX(MALE)`) ~ "YES",
                  TRUE ~ NA_character_
               ),
               SEXFEMALE           = case_when(
                  !is.na(`DATEOFLASTSEX(FEMALE)`) ~ "YES",
                  !is.na(`DATEOFLASTUNPROTECTEDSEX(FEMALE)`) ~ "YES",
                  TRUE ~ NA_character_
               ),

               REASONFORTESTING    = toupper(REASONFORTESTING),
               REASONRECOMMENDED   = if_else(str_detect(REASONFORTESTING, "RECOMMENDED"), "YES", NA_character_),
               REASONPEERED        = if_else(str_detect(REASONFORTESTING, "REF") & str_detect(REASONFORTESTING, "PEER"), "YES", NA_character_),
               REASONHIV           = if_else(str_detect(REASONFORTESTING, "POSSIBLE EXPOSURE"), "YES", NA_character_),
               REASONLOCAL         = if_else(str_detect(REASONFORTESTING, "EMPLOY") & str_detect(REASONFORTESTING, "LOCAL"), "YES", NA_character_),
               REASONOVERSEAS      = if_else(str_detect(REASONFORTESTING, "EMPLOY") & str_detect(REASONFORTESTING, "OVERSEAS"), "YES", NA_character_),
               REASONTEXT          = if_else(str_detect(REASONFORTESTING, "TEXT"), "YES", NA_character_),
               REASONINSURANCE     = if_else(str_detect(REASONFORTESTING, "INSURANCE"), "YES", NA_character_),

               PREVIOUSLYTESTED    = if_else(!is.na(PREVIOUSTESTDATE) | !is.na(PREVIOUSTESTRESULTS), "YES", NA_character_),
               TAKINGPREP          = case_when(
                  PREPUPDATES == "INITIAL" ~ "YES",
                  PREPUPDATES == "ON PREP" ~ "YES",
                  PREPUPDATES == "REFILL" ~ "YES",
                  TRUE ~ NA_character_
               ),

               CLINICALREACH       = if_else(MODEOFREACH == "CLINICAL", "YES", NA_character_),
               OUTREACH            = if_else(MODEOFREACH == "OUTREACH", "YES", NA_character_),

               CONDOMS             = if_else(`CONDOMS&LUBES` == "YES", 3, NA_integer_),
               LUBES               = if_else(`CONDOMS&LUBES` == "YES", 3, NA_integer_),

               SITE                = case_when(
                  Branch == 'ATHENA' ~ 'LoveYourself, Inc. - Athena',
                  Branch == 'AGAPE' ~ 'LoveYourself, Inc. - Agape',
                  Branch == 'JEM' ~ 'LoveYourself, Inc. - Jem',
                  Branch == 'LILY' ~ 'LoveYourself, Inc. - Lily',
                  Branch == 'VICTORIA' ~ 'LoveYourself, Inc. - Victoria',
                  Branch == 'JEFFERYI' ~ 'LoveYourself, Inc. - Jeffery',
                  Branch == 'HERO' ~ 'LoveYourself, Inc. - Hero',
                  Branch == 'WELCOME' ~ 'LoveYourself, Inc. - Welcome',
                  Branch == 'WHITE HOUSE' ~ 'LoveYourself, Inc. - White House',
                  Branch == 'BAGANI' ~ 'LoveYourself, Inc. - Bagani',
                  Branch == 'CARAVAN' ~ 'LoveYourself, Inc. - Caravan',
                  Branch == 'LUXECARE' ~ 'LoveYourself, Inc. - LuxeCare Shaw',
                  Branch == 'ORANGE' ~ 'LoveYourself, Inc. - Orange',
                  Branch == 'ARMY' ~ 'LoveYourself, Inc. - Army',
                  Branch == 'EMBRACE' ~ 'LoveYourself, Inc. - EMBRACE',
                  Branch == 'ABE' ~ 'LoveYourself, Inc. - Abe',
                  Branch == 'UNI' ~ 'LoveYourself, Inc. - Uni',
                  Branch == 'BALAY MARVI' ~ 'LoveYourself, Inc. - Balay Marvi',
                  Branch == 'LUXECARE ALABANG' ~ 'LoveYourself, Inc. - LuxeCare Alabang',
                  Branch == 'MOCHI' ~ 'LoveYourself, Inc. - MOCHI',
                  Branch == 'ANGLO' ~ 'LoveYourself, Inc. - Anglo',
               )
            ) %>%
            separate_wider_delim(
               `CITY/MUNICIPALITYOFRESIDENCE(CURRENT)`,
               ", ",
               names    = c("CURR_MUNC", "CURR_PROV"),
               too_few  = "align_start",
               too_many = "merge"
            ) %>%
            separate_wider_delim(
               `CITY/MUNICIPALITYOFRESIDENCE(PERMANENT)`,
               ", ",
               names    = c("PERM_MUNC", "PERM_PROV"),
               too_few  = "align_start",
               too_many = "merge"
            ) %>%
            mutate(
               ENCODEON          = NA_character_,
               CONSENTSIGNED     = NA_character_,
               CONSENTVERBAL     = NA_character_,
               PHILHEALTH        = NA_character_,
               PHILSYS           = NA_character_,
               CONFIRMATORY      = NA_character_,
               AGEMO             = NA_character_,
               BIRTH_REG         = NA_character_,
               BIRTH_PROV        = NA_character_,
               BIRTH_CURR        = NA_character_,
               NATIONALITY       = NA_character_,
               CIVILSTATUS       = NA_character_,
               LIVINGWITHPARTNER = NA_character_,
               CHILDREN          = NA_character_,
               EDUCLEVEL         = NA_character_,
               INSCHOOL          = NA_character_,
               WORKING           = NA_character_,
               WORK              = NA_character_,
               OFW5              = NA_character_,
               YEARRET           = NA_character_,
               OFWBASED          = NA_character_,
               COUNTRYBASED      = NA_character_,
               MOMHIV            = NA_character_,
               PREVIOUSSITE      = NA_character_,
               TBPX              = NA_character_,
               HEPB              = NA_character_,
               HEPC              = NA_character_,
               STIS              = NA_character_,
               TAKINGPEP         = NA_character_,
               CLINICALPIC       = NA_character_,
               SX                = NA_character_,
               WHOSTAGING        = NA_character_,
               CLIENTTYPE        = NA_character_,
               VENUEREGION       = NA_character_,
               VENUEPROVINCE     = NA_character_,
               VENUEMUNICIPALITY = NA_character_,
               INDEX             = NA_character_,
               SSNT              = NA_character_,
               OUTREACH          = NA_character_,
               REFUSED           = NA_character_,
               ART               = NA_character_,
               CONFIRM           = NA_character_,
               RETESTMO          = NA_character_,
               RETESTWK          = NA_character_,
               IEC               = NA_character_,
               RISKREDUCE        = NA_character_,
               SSNTOFFER         = NA_character_,
               SSNTACCEPT        = NA_character_,
               OTHERSERVICES     = NA_character_,
               PROVIDERTYPE      = NA_character_,
               PROVIDERTYPEOTHER = NA_character_,
            ) %>%
            select(
               `Encoded On`                                                  = `ENCODEON`,
               `Encoded By: (Full Name)`                                     = `ENCODEBY`,
               `Signed Consent`                                              = `CONSENTSIGNED`,
               `Verbal Consent`                                              = `CONSENTVERBAL`,
               `1. Date of Test/Reach`                                       = `DATEOFVISIT`,
               `2. PhilHealth No.`                                           = `PHILHEALTH`,
               `3. PhilSys ID`                                               = `PHILSYS`,
               `HIV Confirmatory Code`                                       = `CONFIRMATORY`,
               `Patient Code`                                                = `ACCESSIONCODE(IFAPPLICABLE)`,
               `4. First Name`                                               = `FIRSTNAME`,
               `Middle Name`                                                 = `MIDDLENAME`,
               `Last Name`                                                   = `LASTNAME`,
               `Suffix (Jr., Sr., III, etc.)`                                = `SUFFIX(JR/SR/ETC.)`,
               `5. First 2 letters of Mother's FIRST Name`                   = `UIC_MOM`,
               `First 2 letters of Father's FIRST Name`                      = `UIC_DAD`,
               `Birth\nOrder`                                                = `UIC_ORDER`,
               `Month`                                                       = `UIC_MO`,
               `Day`                                                         = `UIC_DY`,
               `Year`                                                        = `UIC_YR`,
               `UIC`                                                         = `UNIQUEIDENTIFIERCODE(UIC)`,
               `6. Birth Date`                                               = `BIRTHDATE(AUTO)`,
               `Age (in years)`                                              = `AGE(AUTO)`,
               `Age (in months)`                                             = `AGEMO`,
               `7. Sex (at birth)`                                           = `SEXATBIRTH`,
               `Gender Identity`                                             = `GENDERIDENTITY`,
               `Gender Identity (Others)`                                    = `GENDERIDENTITYOTHER`,
               `8. Current Residence: Region`                                = `CURR_REG`,
               `Current Residence: Province`                                 = `CURR_PROV`,
               `Current Residence:  City/Municipality`                       = `CURR_MUNC`,
               `Permanent Residence: Region`                                 = `PERM_REG`,
               `Permanent Residence: Province`                               = `PERM_PROV`,
               `Permanent Residence:  City/Municipality`                     = `PERM_MUNC`,
               `Place of Birth: Region`                                      = `BIRTH_REG`,
               `Place of Birth: Province`                                    = `BIRTH_PROV`,
               `Place of Birth:  City/Municipality`                          = `BIRTH_CURR`,
               `9. Nationality`                                              = `NATIONALITY`,
               `10. Civil Status`                                            = `CIVILSTATUS`,
               `11. Currently living with a partner?`                        = `LIVINGWITHPARTNER`,
               `No. Of Children`                                             = `CHILDREN`,
               `12. Currently Pregnant?`                                     = `PREGNANT(YES/NO)FEMALES`,
               `13. Highest Education Attainment`                            = `EDUCLEVEL`,
               `14. Currently in school?`                                    = `INSCHOOL`,
               `15. Currently working?`                                      = `WORKING`,
               `Occupation`                                                  = `WORK`,
               `16. Worked/Resided abroad/overseas in the past 5 years?`     = `OFW5`,
               `Year last returned`                                          = `YEARRET`,
               `Where were you based?`                                       = `OFWBASED`,
               `Country last worked in`                                      = `COUNTRYBASED`,
               `17. Birth mother had HIV`                                    = `MOMHIV`,
               `Sex w/ Male (Yes/No)`                                        = `SEXMALE`,
               `Anal/Neovaginal Sex w/ Male (Date Most Recent)`              = `DATEOFLASTSEX(MALE)`,
               `Anal/Neovaginal Sex w/ Male (Date Most Recent Condomless)`   = `DATEOFLASTUNPROTECTEDSEX(MALE)`,
               `Sex w/ Female (Yes/No)`                                      = `SEXFEMALE`,
               `Anal/Neovaginal Sex w/ Female (Date Most Recent)`            = `DATEOFLASTSEX(FEMALE)`,
               `Anal/Neovaginal Sex w/ Female (Date Most Recent Condomless)` = `DATEOFLASTUNPROTECTEDSEX(FEMALE)`,
               `Paid for Sex (Yes/No)`                                       = `PAIDFORSEX?(Y/N)`,
               `Paid for Sex (Date Most Recent)`                             = `DATEOFLASTPAIDSEX`,
               `Paying for Sex (Yes/No)`                                     = `RECEIVEDPAYMENT?(Y/N)`,
               `Paying for Sex (Date Most Recent)`                           = `DATEOFLASTSEXWORK`,
               `Sex under influence of drugs (Yes/No)`                       = `USEDDRUGS?(Y/N)`,
               `Sex under influence of drugs (Date Most Recent)`             = `DATEOFLASTSEXWITHDRUGS`,
               `Shared needles during drug injection (Yes/No)`               = `SHAREDNEEDLES?(Y/N)`,
               `Shared needles during drug injection (Date Most Recent)`     = `DATEOFLASTNEEDLESHARING`,
               `Received blood transfusion (Yes/No)`                         = `RECEIVEDBLOOD?(Y/N)`,
               `Received blood transfusion (Date Most Recent)`               = `DATEOFLASTBLOODTRANSFUSION`,
               `Occupational Exposure `                                      = `OCCUPATIONALEXPOSURE?(Y/N)`,
               `Occupational Exposure  (Date Most Recent)`                   = `DATEOFLASTOCCUPATIONALEXPOSURE`,
               `18. Possible exposure to HIV`                                = `REASONHIV`,
               `Recommended by physician/nurse/midwife`                      = `REASONRECOMMENDED`,
               `Referred by a peer educator`                                 = `REASONPEERED`,
               `Employment - Overseas`                                       = `REASONOVERSEAS`,
               `Employment - Local`                                          = `REASONLOCAL`,
               `Received a text message/email`                               = `REASONTEXT`,
               `Requirement for insurance`                                   = `REASONINSURANCE`,
               `Other reasons (Specify)`                                     = `REASONFORTESTING`,
               `19. Ever been tested for HIV?`                               = `PREVIOUSLYTESTED`,
               `Date of most recent HIV test`                                = `PREVIOUSTESTDATE`,
               `Site/Organization or City/Municipality where you got tested` = `PREVIOUSSITE`,
               `Result of last test`                                         = `PREVIOUSTESTRESULTS`,
               `20. Current TB patient`                                      = `TBPX`,
               `With Hepatitis B`                                            = `HEPB`,
               `With Hepatitis C`                                            = `HEPC`,
               `Diagnosed with other STIs`                                   = `STIS`,
               `Taken PEP`                                                   = `TAKINGPEP`,
               `Taking PrEP`                                                 = `TAKINGPREP`,
               `21. Clinical Picture`                                        = `CLINICALPIC`,
               `Describe S/Sx`                                               = `SX`,
               `WHO Staging`                                                 = `WHOSTAGING`,
               `22. Client Type`                                             = `CLIENTTYPE`,
               `Venue: Region`                                               = `VENUEREGION`,
               `Venue: Province`                                             = `VENUEPROVINCE`,
               `Venue: City/Municipality`                                    = `VENUEMUNICIPALITY`,
               `Venue: Details`                                              = `LOCATION`,
               `23. Clinical`                                                = `CLINICALREACH`,
               `Online`                                                      = `OUTREACH`,
               `Index testing`                                               = `INDEX`,
               `SSNT`                                                        = `SSNT`,
               `Outreach`                                                    = `OUTREACH`,
               `24. HIV Test (Accept/Refuse)`                                = `TESTINGCONSENT`,
               `Reason for refusal`                                          = `REFUSED`,
               `HIV testing modality`                                        = `HIVTESTINGMODALITY`,
               `HIV test result`                                             = `INTERPRETATIONHIV(TEST1)`,
               `Refer to ART`                                                = `ART`,
               `Refer for Confirmatory`                                      = `CONFIRM`,
               `Advise for retesting in: Months`                             = `RETESTMO`,
               `Advise for retesting in: Weeks`                              = `RETESTWK`,
               `25. HIV 101`                                                 = `HIV101`,
               `IEC materials`                                               = `IEC`,
               `Risk reduction planning`                                     = `RISKREDUCE`,
               `Referred to PrEP or given PEP`                               = `OFFEREDPREP?`,
               `Offered SSNT`                                                = `SSNTOFFER`,
               `Accepted SSNT`                                               = `SSNTACCEPT`,
               `Condoms: # distributed`                                      = `CONDOMS`,
               `Lubricants: # distributed`                                   = `LUBES`,
               `Other services`                                              = `OTHERSERVICES`,
               `26. Name of Testing Site/Organization`                       = `SITE`,
               `27. Primary HTS Provider`                                    = `ASSIGNEDCOUNSELOR`,
               `HTS Provider Type`                                           = `PROVIDERTYPE`,
               `HTS Provider Type (Others)`                                  = `PROVIDERTYPEOTHER`,
               `Clinical Notes`                                              = `LINKAGE`,
               `Counseling Notes`                                            = `REMARKS`,
               `Client Email`                                                = `EMAILADDRESS`,
               `Client Mobile`                                               = `CONTACTNUMBER`,
            ) %>%
            mutate_at(
               .vars = vars(contains("Date")),
               ~as.Date(parse_date_time(., c("mdY", "mY", "Ym", "Y")))
            ) %>%
            filter(
               `1. Date of Test/Reach` <= now(),
               `1. Date of Test/Reach` >= as.Date("2025-01-01")
            )

         invisible(self)
      },

      readFile      = function(file) {
         log_info("Reading = {green(tools::file_path_sans_ext(basename(file)))}.")
         sheets      <- intersect(self$months, ods_sheets(file))
         data        <- lapply(sheets, read_ods, path = file, skip = 1, col_types = cols(.default = "c"), .name_repair = "unique_quiet")
         data        <- lapply(data, mutate_all, ~na_if(., "Err:522"))
         data        <- lapply(data, mutate, Row = stri_c("A", row_number() + 1), .before = 1)
         data        <- lapply(data, remove_empty, which = "rows", cutoff = .017)
         data        <- lapply(data, rename_all, ~toupper(stri_replace_all_regex(., "\\s", "")))
         names(data) <- sheets

         return(bind_rows(data, .id = "Sheet"))
      },
      writeLogsheet = function(file) {
         write.xlsx(self$data$logsheet %>% mutate_if(is.Date, as.character), file, startRow = 3, firstActiveRow = 4, colWidths = "auto")
      }
   )
)
