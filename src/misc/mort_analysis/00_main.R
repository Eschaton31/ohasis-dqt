mort_analysis <- hs_data("harp_dead", "reg", 2023, 6) %>%
   read_dta() %>%
   mutate_at(
      .vars = vars(immediate_cause, antecedentcause, underlying_cause, hiv_related_cause, not_hiv_related_cause, other_significant_conditions_1, other_significant_conditions_2, report_notes),
      ~na_if(., "")
   ) %>%
   mutate_at(
      .vars = vars(immediate_cause, antecedentcause, underlying_cause, hiv_related_cause, not_hiv_related_cause, other_significant_conditions_1, other_significant_conditions_2, report_notes),
      ~if_else(StrIsNumeric(.), NA_character_, ., .)
   ) %>%
   unite(
      col    = "cause_concat",
      sep    = "; ",
      immediate_cause, antecedentcause, underlying_cause, hiv_related_cause, not_hiv_related_cause, other_significant_conditions_1, other_significant_conditions_2, report_notes,
      na.rm  = TRUE,
      remove = FALSE
   ) %>%
   mutate(
      cause_concat = str_squish(toupper(cause_concat)),
      cause_none   = if_else(if_all(c(immediate_cause, antecedentcause, underlying_cause), ~. == ""), 1, 0, 0),
      no_date      = if_else(is.na(date_of_death), 1, 0, 0),
      age_grp      = gen_agegrp(age, "harp"),
      year_death   = coalesce(year(date_of_death), year),
      everdx       = if_else(!is.na(idnum), 1, 0, 0)
   ) %>%
   mutate(
      cause_tb                = case_when(
         tb == 1 ~ 1,
         str_detect(cause_concat, "\\bTB\\b") ~ 1,
         str_detect(cause_concat, "\\bETB\\b") ~ 1,
         str_detect(cause_concat, "\\bPTB\\b") ~ 1,
         str_detect(cause_concat, "\\bEPTB\\b") ~ 1,
         str_detect(cause_concat, "\\bTUB.RC.LOSIS\\b") ~ 1,
         str_detect(cause_concat, "TUBER") ~ 1,
         TRUE ~ 0
      ),
      cause_covid             = case_when(
         covid19 == 1 ~ 1,
         str_detect(cause_concat, "\\bCOVID\\b") ~ 1,
         str_detect(cause_concat, "\\bC19\\b") ~ 1,
         TRUE ~ 0
      ),
      cause_sepsis            = case_when(
         str_detect(cause_concat, "\\bSEPSIS\\b") ~ 1,
         str_detect(cause_concat, "\\bSEPTIC\\b") ~ 1,
         TRUE ~ 0
      ),
      cause_immunocompromised = case_when(
         str_detect(cause_concat, "IMMUNO") ~ 1,
         str_detect(cause_concat, "IMMONU") ~ 1,
         TRUE ~ 0
      ),
      cause_neuro             = case_when(
         str_detect(cause_concat, "\\bCNS\\b") ~ 1,
         str_detect(cause_concat, "BRAIN") ~ 1,
         str_detect(cause_concat, "CENTRAL NERV") ~ 1,
         str_detect(cause_concat, "CEREB") ~ 1,
         str_detect(cause_concat, "ENCEPH") ~ 1,
         str_detect(cause_concat, "CRANIAL") ~ 1,
         str_detect(cause_concat, "NEURO") ~ 1,
         TRUE ~ 0
      ),
      cause_meningitis        = case_when(
         cmeningitis == 1 ~ 1,
         str_detect(cause_concat, "MENINGITIS") ~ 1,
         TRUE ~ 0
      ),
      cause_cardio            = case_when(
         str_detect(cause_concat, "CARDIA") ~ 1,
         str_detect(cause_concat, "CARDIO") ~ 1,
         str_detect(cause_concat, "CARDIAC") ~ 1,
         str_detect(cause_concat, "HEART") ~ 1,
         str_detect(cause_concat, "HYPERTENS") ~ 1,
         str_detect(cause_concat, "CORONARY") ~ 1,
         str_detect(cause_concat, "ARRHYTHM") ~ 1,
         str_detect(cause_concat, "\\bARRY") ~ 1,
         str_detect(cause_concat, "PUMP FAIL") ~ 1,
         str_detect(cause_concat, "\\bCAD\\b") ~ 1,
         str_detect(cause_concat, "\\bACS\\b") ~ 1,
         str_detect(cause_concat, "\\bCHF\\b") ~ 1,
         str_detect(cause_concat, "\\bMI\\b") ~ 1,
         TRUE ~ 0
      ),
      cause_pneumonia         = case_when(
         pcp == 1 ~ 1,
         str_detect(cause_concat, "\\bCAP\\b") ~ 1,
         str_detect(cause_concat, "\\bHAP\\b") ~ 1,
         str_detect(cause_concat, "\\bPCP\\b") ~ 1,
         str_detect(cause_concat, "\\bPNEUMONIA\\b") ~ 1,
         str_detect(cause_concat, "\\bPNEUMO") ~ 1,
         str_detect(cause_concat, "\\bPNEMONIA") ~ 1,
         str_detect(cause_concat, "\\bLUNG") ~ 1,
         TRUE ~ 0
      ),
      cause_respiratory       = case_when(
         str_detect(cause_concat, "RESPI") ~ 1,
         str_detect(cause_concat, "\\bARF\\b") ~ 1,
         str_detect(cause_concat, "\\bPULMO") ~ 1,
         str_detect(cause_concat, "ASTHMA") ~ 1,
         str_detect(cause_concat, "MYCOBAC") ~ 1,
         str_detect(cause_concat, "\\bPLEURAL\\b") ~ 1,
         str_detect(cause_concat, "\\bCOPD\\b") ~ 1,
         str_detect(cause_concat, "\\bBREATH") ~ 1,
         str_detect(cause_concat, "HYPOXIA") ~ 1,
         TRUE ~ 0
      ),
      cause_ckd               = case_when(
         str_detect(cause_concat, "\\bCKD\\\b") ~ 1,
         str_detect(cause_concat, "RENAL") ~ 1,
         str_detect(cause_concat, "KIDNEY") ~ 1,
         str_detect(cause_concat, "DIABETIC NEPHROPATHY") ~ 1,
         TRUE ~ 0
      ),
      cause_organ_fail        = case_when(
         str_detect(cause_concat, "ORGAN FAIL") ~ 1,
         TRUE ~ 0
      ),
      cause_accident          = case_when(
         str_detect(cause_concat, "ACCIDENT") ~ 1,
         str_detect(cause_concat, "\\bDROWN") ~ 1,
         str_detect(cause_concat, "DROWN") ~ 1,
         TRUE ~ 0
      ),
      cause_suicide           = case_when(
         str_detect(cause_concat, "HANGING") ~ 1,
         str_detect(cause_concat, "SUICIDE") ~ 1,
         str_detect(cause_concat, "ASPHYXIA") ~ 1,
         TRUE ~ 0
      ),
      cause_cancer            = case_when(
         str_detect(cause_concat, "CANCER") ~ 1,
         str_detect(cause_concat, "\\bCA\\b") ~ 1,
         str_detect(cause_concat, "CARCINOMA") ~ 1,
         str_detect(cause_concat, "LEUKEMIA") ~ 1,
         str_detect(cause_concat, "SARCOMA") ~ 1,
         str_detect(cause_concat, "MALIGNANCY") ~ 1,
         str_detect(cause_concat, "LYMPHO.A") ~ 1,
         TRUE ~ 0
      ),
      cause_liver             = case_when(
         str_detect(cause_concat, "LIVER") ~ 1,
         str_detect(cause_concat, "HEPA") ~ 1,
         TRUE ~ 0
      ),
      cause_diarrhea          = case_when(
         str_detect(cause_concat, "DIARRHEA") ~ 1,
         TRUE ~ 0
      ),
      cause_aids              = case_when(
         str_detect(cause_concat, "AIDS") ~ 1,
         str_detect(cause_concat, "\\bHIV\\b") ~ 1,
         TRUE ~ 0
      ),
      cause_home              = case_when(
         str_detect(cause_concat, "HOME") ~ 1,
         TRUE ~ 0
      ),
      cause_gi                = case_when(
         str_detect(cause_concat, "DIGEST") ~ 1,
         str_detect(cause_concat, "GASTRO") ~ 1,
         str_detect(cause_concat, "INTESTIN") ~ 1,
         str_detect(cause_concat, "LIVER") ~ 1,
         str_detect(cause_concat, "BOWEL") ~ 1,
         str_detect(cause_concat, "\\bAPPENDI") ~ 1,
         str_detect(cause_concat, "\\bPEPTIC") ~ 1,
         str_detect(cause_concat, "\\bHEPAT") ~ 1,
         str_detect(cause_concat, "\\bGI\\b") ~ 1,
         TRUE ~ 0
      ),
      cause_unknown           = case_when(
         cause_concat == "" ~ 1,
         is.na(cause_concat) ~ 1,
         str_detect(cause_concat, "UNKNOWN") ~ 1,
         str_detect(cause_concat, "^DIED AT") ~ 1,
         str_detect(cause_concat, "^REPORTED") ~ 1,
         TRUE ~ 0
      ),
      cause_hep               = case_when(
         hepc == 1 ~ 1,
         hepb == 1 ~ 1,
         str_detect(cause_concat, "HEPATITIS") ~ 1,
         str_detect(cause_concat, "\\bHEP\\b") ~ 1,
         TRUE ~ 0
      ),
      cause_candidiasis       = case_when(
         candidiasis == 1 ~ 1,
         str_detect(cause_concat, "CANDIDIA") ~ 1,
         TRUE ~ 0
      ),
      cause_trauma            = case_when(
         str_detect(cause_concat, "\\bSTAB") ~ 1,
         str_detect(cause_concat, "\\bGUN *SHOT") ~ 1,
         str_detect(cause_concat, "TRAUMA") ~ 1,
         TRUE ~ 0
      ),
      cause_cmv               = case_when(
         cmv == 1 ~ 1,
         str_detect(cause_concat, "\\bCMV") ~ 1,
         str_detect(cause_concat, "\\bCYTOMEGALO") ~ 1,
         TRUE ~ 0
      ),
      cause_shock             = case_when(
         str_detect(cause_concat, "SHOCK") ~ 1,
         TRUE ~ 0
      ),
   )

mort_analysis %>%
   select(
      mort_id,
      starts_with("cause_")
   ) %>%
   write_sheet("13613FmbewzX-nAKpRHU7T74IrCDoIjtoXdoCB87n8vc", "ll causes_cat new")

mort_analysis %>%
   select(
      mort_id,
      cause_concat
   ) %>%
   separate_longer_delim(
      cause_concat,
      " "
   ) %>%
   filter(cause_concat != "") %>%
   tab(cause_concat, as_df = TRUE) %>%
   arrange(desc(Freq.)) %>%
   write_sheet("13613FmbewzX-nAKpRHU7T74IrCDoIjtoXdoCB87n8vc", "word frequency")