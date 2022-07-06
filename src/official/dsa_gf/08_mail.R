##  Aggregates -----------------------------------------------------------------

gf$as_of <- as.POSIXct(
   paste0(
      strsplit(ohasis$timestamp, "\\.")[[1]][1], "-",
      strsplit(ohasis$timestamp, "\\.")[[1]][2], "-",
      strsplit(ohasis$timestamp, "\\.")[[1]][3], " ",
      StrLeft(strsplit(ohasis$timestamp, "\\.")[[1]][4], 2), ":",
      substr(strsplit(ohasis$timestamp, "\\.")[[1]][4], 3, 4), ":",
      StrRight(strsplit(ohasis$timestamp, "\\.")[[1]][4], 2), ":"
   )
)

gf$mail$title <- glue("PSFI-PROTECTS Project Indicators for GF-Supported Sites ({gf$coverage$curr_yr}-{gf$coverage$curr_mo})")

gf$mail$to <- c(
   "crmaranan@pilipinasshellfoundation.org",
   "lbpnorella@pilipinasshellfoundation.org",
   "agdelacruz@pilipinasshellfoundation.org",
   "me.hiv@pilipinasshellfoundation.org"
)

gf$mail$for_checking <- gm_mime() %>%
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
   gm_subject(glue("[For Checking] {gf$mail$title}")) %>%
   gm_html_body(
      glue(
         r"(
<p>Good day,</p>
<p>You are receiving this message because you are a member of one of the HIV surveillance systems utilized in generating the PSFI-PROTECTS Project Indicators.
<p>This communication is specifically for the monthly indicators and the required counter-checking and validation to be conducted prior to releasing to the GF M&E Team.</p>
<p>For the staff new to the MER Indicators CC, reach out to your seniors to provide context on what is to be done.</p>
<p>Attached are the files pertaining to the subject above. Kindly conduct the necessary checking. Final release is slated to be before COB today, May 10.</p>
{gmail_sig()}
         )"
      )
   ) %>%
   gm_attach_file(file.path(gf$xlsx$dir, gf$xlsx$file))

gf$mail$for_approval <- gm_mime() %>%
   gm_to(gmail$nhsss$head) %>%
   gm_cc(
      unique(
         c(
            gmail$nhsss$ss,
            gmail$nhsss$dqt,
            gmail$nhsss$dat
         )
      )
   ) %>%
   gm_from(Sys.getenv("GMAIL_USER")) %>%
   gm_subject(glue("[For Approval] {gf$mail$title}")) %>%
   gm_html_body(
      glue(
         r"(
<p>Hi Sir Noel & Ate Ja,</p>
<p>Attached here is the flat file for the aforementioned indicators set to be released to the GF M&E Team. All indicators were prepared by myself.</p>
<p>Once you provide approval, I will release ASAP to the GF M&E Team.</p>
{gmail_sig()}
         )"
      )
   ) %>%
   gm_attach_file(file.path(gf$xlsx$dir, gf$xlsx$file))

gf$mail$for_release <- gm_mime() %>%
   gm_to(
      gf$mail$to
   ) %>%
   gm_cc(
      unique(
         c(
            gmail$nhsss$ss,
            gmail$nhsss$dqt,
            gmail$nhsss$dat
         )
      )
   ) %>%
   gm_from(Sys.getenv("GMAIL_USER")) %>%
   gm_subject(glue("[Data Release] {gf$mail$title}")) %>%
   gm_html_body(
      glue(
         r"(
<p>Hi GF-PROTECTS M&E Team,</p>
<p>Attached here are the monthly PSFI-PROTECTS Project Indicators for GF-Supported Sites as of <b>{month.name[as.numeric(gf$coverage$curr_mo)]} {gf$coverage$curr_yr}</b>. OHASIS data was extracted as of <b>{format(gf$as_of, "%a %b %d, %Y %X")}</b>.</p>
<p>Aggregates were geenerated using the combined data of the Raw M&E Logsheet and the OHASIS Extract. Clients were linked via UIC & Facility to find the overlaps, with subsequent deduplication and linkage for the Diagnosis, Treatment, and PrEP Data.</p>
<p>Should you have any questions or clarifications, please don\'t hesitate to let me know.</p>
{gmail_sig()}
         )"
      )
   ) %>%
   gm_attach_file(file.path(gf$xlsx$dir, gf$xlsx$file))

##  Logsheet -----------------------------------------------------------------

gf$as_of <- as.POSIXct(
   paste0(
      strsplit(ohasis$timestamp, "\\.")[[1]][1], "-",
      strsplit(ohasis$timestamp, "\\.")[[1]][2], "-",
      strsplit(ohasis$timestamp, "\\.")[[1]][3], " ",
      StrLeft(strsplit(ohasis$timestamp, "\\.")[[1]][4], 2), ":",
      substr(strsplit(ohasis$timestamp, "\\.")[[1]][4], 3, 4), ":",
      StrRight(strsplit(ohasis$timestamp, "\\.")[[1]][4], 2), ":"
   )
)

gf$mail$title <- glue("PSFI-PROTECTS Logsheets for GF-Supported Sites w/ DSA ({gf$coverage$curr_yr}-{gf$coverage$curr_mo})")

gf$mail$to <- c(
   "crmaranan@pilipinasshellfoundation.org",
   "lbpnorella@pilipinasshellfoundation.org",
   "agdelacruz@pilipinasshellfoundation.org",
   "me.hiv@pilipinasshellfoundation.org"
)

gf$mail$for_checking <- gm_mime() %>%
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
   gm_subject(glue("[For Checking] {gf$mail$title}")) %>%
   gm_html_body(
      glue(
         r"(
<p>Good day,</p>
<p>You are receiving this message because you are a member of one of the HIV surveillance systems utilized in generating the PSFI-PROTECTS Project Indicators.
<p>This communication is specifically for the monthly indicators and the required counter-checking and validation to be conducted prior to releasing to the GF M&E Team.</p>
<p>For the staff new to the MER Indicators CC, reach out to your seniors to provide context on what is to be done.</p>
<p>Attached are the files pertaining to the subject above. Kindly conduct the necessary checking. Final release is slated to be before COB today, May 10.</p>
{gmail_sig()}
         )"
      )
   ) %>%
   gm_attach_file(file.path(gf$xlsx$dir, gf$xlsx$file))

gf$mail$for_approval <- gm_mime() %>%
   gm_to(gmail$nhsss$head) %>%
   gm_cc(
      unique(
         c(
            gmail$nhsss$ss,
            gmail$nhsss$dqt,
            gmail$nhsss$dat
         )
      )
   ) %>%
   gm_from(Sys.getenv("GMAIL_USER")) %>%
   gm_subject(glue("[For Approval] {gf$mail$title}")) %>%
   gm_html_body(
      glue(
         r"(
<p>Hi Sir Noel & Ate Ja,</p>
<p>Attached here are the logsheet files set to be released to the GF M&E Team. All data were prepared by myself. Data has been limited to those who have submitted a DSA between their faciity and PSFI.</p>
<p>Once you provide approval, I will release ASAP to the GF M&E Team.</p>
{gmail_sig()}
         )"
      )
   ) %>%
   gm_attach_file("H:/Data Sharing/PSFI/2022.06/ohasis_logsheet_2022-06.xlsx") %>%
   gm_attach_file("H:/Data Sharing/PSFI/2022.06/ohasis_logsheet_2022-06.dta")

gf$mail$for_release <- gm_mime() %>%
   gm_to(
      gf$mail$to
   ) %>%
   gm_cc(
      unique(
         c(
            gmail$nhsss$head,
            gmail$nhsss$ss,
            gmail$nhsss$dqt,
            gmail$nhsss$dat
         )
      )
   ) %>%
   gm_from(Sys.getenv("GMAIL_USER")) %>%
   gm_subject(glue("[Data Release] {gf$mail$title}")) %>%
   gm_html_body(
      glue(
         r"(
<p>Hi GF-PROTECTS M&E Team,</p>
<p>Attached here are the monthly PSFI-PROTECTS Logsheet data for GF-Supported Sites w/ DSA as of <b>{month.name[as.numeric(gf$coverage$curr_mo)]} {gf$coverage$curr_yr}</b>. OHASIS data was extracted as of <b>{format(gf$as_of, "%a %b %d, %Y %X")}</b>.</p>
<p>Should you have any questions or clarifications, please don\'t hesitate to let me know.</p>
{gmail_sig()}
         )"
      )
   )%>%
   gm_attach_file("H:/Data Sharing/PSFI/2022.06/ohasis_logsheet_2022-06.xlsx") %>%
   gm_attach_file("H:/Data Sharing/PSFI/2022.06/ohasis_logsheet_2022-06.dta")
