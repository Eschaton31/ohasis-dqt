hubml      <- new.env()
hubml$file <- "H:/2019.10_hubml - slh.xlsx"
hubml$data <- read_worksheet(hubml$file, "Sheet1")


try <- hubml$data %>%
   mutate(
      CONFIRMATORY_CODE = coalesce(SACCL.CODE, CONFIRM_CODE),
      PATIENT_CODE      = str_c(coalesce(H4.CODE, ""), " ", coalesce(PATIENT.CODE, "")),
      UIC               = case_when(
         str_detect(UIC, "^H[0-9]") ~ NA_character_,
         !str_detect(UIC, "[0-9]") ~ NA_character_,
         TRUE ~ UIC
      )
   ) %>%
   select(
      CENTRAL_ID,
      DATE_CONFIRM     = CONFIRMATORY.DATE,
      CONFIRMATORY_CODE,
      ML_STATUS        = ML.Status,
      FIRST_VISIT_DATE = DATE.OF.FIRST.VISIT,
      UIC,
      PATIENT_CODE,
      FIRST,
      MIDDLE,
      LAST,
      SUFFIX,
      AGE,
      SEX,
      BIRTHDATE        = BDAY,
      ADDRESS,
      BIRTHPLACE       = BPLACE,
      CIVIL_STATUS     = CS,
      OCCUPATION,
      HX_TRAVEL        = HX.OF.TRAVEL,
      MOT              = MODE.OF.TRANSMISSION,
      VL_RESULT        = VIRAL.LOAD,
      CD4_RESULT       = CD4,
      XPERT_RESULT     = GENE.EXPERT.SPUTUM,
      SPUTUM_RESULT    = SPUTUM,
      XRAY_RESULT      = XRAY,
      OI,
      MEDS,
      CLIENT_TYPE      = WARD.OR.OPD,
      CASE_MANAGER_1   = X.AGENT,
      CASE_MANAGER_2   = X.AGENT.1,
      OTHERS,
      REMARKS          = REMARKS.1
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(.) %>%
         if_else(. == "", NA_character_, ., .)
   ) %>%
   mutate_at(
      .vars = vars(contains("DATE")),
      ~as.Date(.)
   ) %>%
   filter(if_any(c(FIRST, MIDDLE, LAST, SEX, BIRTHDATE, PATIENT_CODE), ~!is.na(.)))

parse_ph_addr <- function(data, address, region, province, muncity) {
   col_addr <- as.name(address)
   col_reg  <- as.name(region)
   col_prov <- as.name(province)
   col_munc <- as.name(muncity)

   data %<>%
      mutate_at(
         .vars = vars({{col_addr}}),
         ~stri_trans_general(str_squish(stri_trans_toupper(.)), "latin-ascii") %>%
            str_replace_all("[^[:alnum:] ]", "") %>%
            str_replace("^CITY OF ", "") %>%
            str_replace("\\bCITY$", "") %>%
            str_replace("\\bCIITY", "") %>%
            str_replace("\\bITY", "") %>%
            str_replace("STA\\b", "SANTA") %>%
            str_replace("STO\\b", "SANTO") %>%
            str_replace("GEN\\b", "GENERAL") %>%
            str_squish()
      ) %>%
      mutate(
         .after       = {{col_addr}},
         {{col_munc}} := case_when(
            {{col_addr}} %in% c("MANILA", "MANDALUYONG", "MARIKINA", "PASIG", "QUEZON", "SAN", "JUAN", "CALOOCAN", "MALABON", "NAVOTAS", "VALENZUELA", "LAS", "PIÑAS", "MAKATI", "MUNTINLUPA", "PARAÑAQUE", "PASAY", "PATEROS", "TAGUIG") ~ {{col_addr}},
            {{col_addr}} %in% c("KALOOKAN", "KALOOCAN") ~ "CALOOCAN",
            {{col_addr}} == "CAVITE" ~ "CAVITE",
            str_detect({{col_addr}}, "\\TONDO\\b") ~ "MANILA",
            str_detect({{col_addr}}, "\\SAMPALOC\\b") ~ "MANILA",
            str_detect({{col_addr}}, "\\QUIAPO\\b") ~ "MANILA",
            str_detect({{col_addr}}, "\\bCALOOCAN\\b") ~ "CALOOCAN",
            str_detect({{col_addr}}, "\\bALABANG\\b") ~ "MUNTINLUPA",
            str_detect({{col_addr}}, "\\BACOOR\\b") ~ "BACOOR",
            str_detect({{col_addr}}, "\\bDASMARINAS\\b") ~ "DASMARIÑAS",
            str_detect({{col_addr}}, "\\bDASMA\\b") ~ "DASMARIÑAS",
            str_detect({{col_addr}}, "\\bIMUS\\b") ~ "IMUS",
            str_detect({{col_addr}}, "\\bQC\\b") ~ "QUEZON",
            TRUE ~ NA_character_
         ),
         {{col_prov}} := case_when(
            {{col_munc}} %in% c("MANILA", "MANDALUYONG", "MARIKINA", "PASIG", "QUEZON", "SAN", "JUAN", "CALOOCAN", "MALABON", "NAVOTAS", "VALENZUELA", "LAS", "PIÑAS", "MAKATI", "MUNTINLUPA", "PARAÑAQUE", "PASAY", "PATEROS", "TAGUIG") ~ "NCR",
            {{col_munc}} %in% c("BACOOR", "CAVITE", "IMUS", "DASMARIÑAS") ~ "CAVITE",
            TRUE ~ NA_character_
         ),
         {{col_reg}}  := case_when(
            {{col_munc}} %in% c("MANILA", "MANDALUYONG", "MARIKINA", "PASIG", "QUEZON", "SAN", "JUAN", "CALOOCAN", "MALABON", "NAVOTAS", "VALENZUELA", "LAS", "PIÑAS", "MAKATI", "MUNTINLUPA", "PARAÑAQUE", "PASAY", "PATEROS", "TAGUIG") ~ "NCR",
            {{col_munc}} %in% c("BACOOR", "CAVITE", "IMUS", "DASMARIÑAS") ~ "4A",
            TRUE ~ NA_character_
         )
      )

   return(data)
}

parse_ph_addr(try, "ADDRESS", "reg", "prov", "munc") %>%
   tab(reg, prov, munc, ADDRESS)