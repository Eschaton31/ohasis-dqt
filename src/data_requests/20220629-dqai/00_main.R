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
rm(lw_conn)

##  HARP -----------------------------------------------------------------------

dqai$harp$tx$reg <- ohasis$get_data("harp_tx-reg", "2022", "03") %>%
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

dqai$harp$tx$outcome_mar <- ohasis$get_data("harp_tx-outcome", "2022", "03") %>%
   read_dta() %>%
   mutate_if(
      .predicate = is.character,
      ~zap_empty(.)
   )

dqai$harp$tx$outcome_may <- ohasis$get_data("harp_tx-outcome", "2022", "05") %>%
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
         dqai$harp$tx$outcome_mar %>%
            filter(realhub == faci_code | hub == faci_code) %>%
            select(art_id),
         dqai$harp$tx$outcome_may %>%
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
         y  = dqai$harp$tx$outcome_mar %>%
            select(
               art_id,
               age_202203     = curr_age,
               ffup_202203    = latest_ffupdate,
               pickup_202203  = latest_nextpickup,
               hub_202203     = hub,
               outcome_202203 = outcome
            ),
         by = "art_id"
      ) %>%
      left_join(
         y  = dqai$harp$tx$outcome_may %>%
            select(
               art_id,
               age_202205     = curr_age,
               ffup_202205    = latest_ffupdate,
               pickup_202205  = latest_nextpickup,
               hub_202205     = realhub,
               branch_202205  = realhub_branch,
               outcome_202205 = outcome
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


invisible(lapply(names(dqai$faci), function(faci_code) {
   if (!is.na(faci_code) & faci_code != "NA") {
      file_name                         <- glue("H:/Events/DQAI/2022/Assessment Tool/Datasets/{tolower(faci_code)}_tx_clients-dqai_2022-03.dta")
      .GlobalEnv$dqai$faci[[faci_code]] <- read_dta(file_name) %>%
         mutate_if(
            .predicate = is.character,
            ~zap_empty(.)
         ) %>%
         mutate(HUB = faci_code)
   }
}))

dqai$flat <- bind_rows(as.list(dqai$faci)) %>%
   mutate(
      diff           = floor(difftime(as.Date("2022-03-31"), pickup_202203, units = "days")),
      outcome_202203 = case_when(
         outcome_202203 == "alive on arv" ~ "onart",
         outcome_202203 == "lost to follow up" ~ "ltfu",
         outcome_202203 == "trans out" ~ "transout",
         TRUE ~ outcome_202203
      ),
      new            = if_else(
         condition = year(artstart_date) >= 2021 & HUB == artstart_hub,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      alive          = if_else(
         condition = HUB == hub_202203 & outcome_202203 == "onart",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      ltfu           = if_else(
         condition = HUB == hub_202203 & outcome_202203 == "ltfu",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      dead           = if_else(
         condition = outcome_202203 == "dead",
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      transout       = if_else(
         condition = HUB != hub_202203,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      transin        = if_else(
         condition = HUB != artstart_hub,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      stopped        = if_else(
         condition = stri_detect_fixed(outcome_202203, "stopped"),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
   ) %>%
   group_by(HUB) %>%
   summarise(
      `Total Number of Clients` = n(),
      `New on ARV`              = sum(new),
      `Alive on ART`            = sum(alive),
      `LTFU`                    = sum(ltfu),
      `Mortality`               = sum(dead),
      `Trans-Out`               = sum(transout),
      `Trans-In`                = sum(transin),
      `Stopped`                 = sum(stopped),
   ) %>%
   ungroup() %>%
   mutate_if(
      .predicate = is.numeric,
      ~as.integer(.)
   ) %>%
   left_join(
      y  = ohasis$ref_faci_code %>%
         select(
            HUB      = FACI_CODE,
            Facility = FACI_NAME
         ) %>%
         distinct(HUB, .keep_all = TRUE) %>%
         mutate(
            Facility = case_when(
               Facility == "LoveYourself, Inc. (Hero)" ~ "LoveYourself, Inc. (Anglo)",
               Facility == "De La Salle Medical and Health Sciences Institute" ~ "De La Salle Medical and Health Sciences Institute (De La Salle University Medical Center)",
               Facility == "Visayas Med-Cebu Hospital" ~ "Visayas Community Medical Center (Balay Malingkawasnon)",
               TRUE ~ Facility
            )
         ),
      by = "HUB"
   )

##  Write to stata files -------------------------------------------------------

invisible(lapply(names(dqai$faci), function(faci_code) {
   if (!is.na(faci_code)) {
      file_name <- glue("H:/Events/DQAI/2022/Assessment Tool/Datasets/{tolower(faci_code)}_tx_clients-dqai_2022-03.dta")
      write_dta(
         dqai$faci[[faci_code]],
         file_name
      )
   }
}))

##  Send to team ---------------------------------------------------------------

dqai$mail <- gm_mime() %>%
   gm_to(
      unique(
         c(
            gmail$nhsss$ss,
            gmail$nhsss$dqt,
            gmail$nhsss$dat
         )
      )
   ) %>%
   gm_from(Sys.getenv("GMAIL_USER")) %>%
   gm_subject(glue("[Data Set] DQAI Assessment Tool - Treatment")) %>%
   gm_html_body(
      glue(
         r"(
<p>Hi guys,</p>
<p>Attached are the datasets for generating the data needed for the DQAI Assement Tool. Generate the indicators based on the descriptions provided in the tool and what we'd discussed earlier.</p>
<p>As discussed, I expect your output by tomorrow, June 30, 2022 at 12NN. Looking forward to your output.</p>
<p>I didn't provide a codebook since the variables are pretty self-explanatory. Feel free to reach out via this thread should you have any further queries. Best of luck.</p>
{gmail_sig()}
         )"
      )
   ) %>%
   gm_attach_file("H:/Events/DQAI/2022/Assessment Tool/dqai_assesment_tool_datasets.zip")

dqai$mail <- gm_mime() %>%
   gm_to(
      unique(
         c(
            gmail$nhsss$ss,
            gmail$nhsss$dqt,
            gmail$nhsss$dat
         )
      )
   ) %>%
   gm_from(Sys.getenv("GMAIL_USER")) %>%
   gm_subject(glue("[Data Set] DQAI Assessment Tool - Treatment")) %>%
   gm_html_body(
      glue(
         r"(
<p>Apologies, seemed to have included the April-May 2022 new clients in the previous datasets. Re-sending here the updated versions w/o them.</p>
<p>Best,<br>- Bene</p>
{gmail_sig()}
         )"
      )
   ) %>%
   gm_attach_file("H:/Events/DQAI/2022/Assessment Tool/dqai_assesment_tool_datasets_v2.zip")

gm_send_message(dqai$mail, thread_id = "181ae726e104b9ce")

dqai$mail <- gm_mime() %>%
   gm_to(
      unique(
         c(
            gmail$nhsss$ss,
            gmail$nhsss$dqt,
            gmail$nhsss$dat
         )
      )
   ) %>%
   gm_from(Sys.getenv("GMAIL_USER")) %>%
   gm_subject(glue("[Data Set] DQAI Assessment Tool - Treatment")) %>%
   gm_html_body(
      glue(
         r"(
<p>Hi guys,</p>
<p>Thanks for providing your output. I've proceeded to check the data you placed into the GSheet created by Kath yesterday. You've now been restricted in editing the sheet to preserve the correctness of your inputs at the end of the deadline.</p>
<p><b>Green</b> is correct, <b>Purple</b> is wrong. Look to your seniors for guidance and feedback regarding this. I expect to speak with you after my meetings, as well.</p>
<p>Best,<br>- Bene</p>
{gmail_sig()}
         )"
      )
   )
gm_send_message(dqai$mail, thread_id = "181ae726e104b9ce")