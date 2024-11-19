##  Aggregates -----------------------------------------------------------------

icap$as_of <- as.POSIXct(
   paste0(
      strsplit(ohasis$timestamp, "\\.")[[1]][1], "-",
      strsplit(ohasis$timestamp, "\\.")[[1]][2], "-",
      strsplit(ohasis$timestamp, "\\.")[[1]][3], " ",
      str_left(strsplit(ohasis$timestamp, "\\.")[[1]][4], 2), ":",
      substr(strsplit(ohasis$timestamp, "\\.")[[1]][4], 3, 4), ":",
      StrRight(strsplit(ohasis$timestamp, "\\.")[[1]][4], 2), ":"
   )
)

icap$mail$title <- glue("ICAP {icap$coverage$type} Indicators for {stri_replace_all_fixed(icap$coverage$ym, '.', '-')}")

icap$mail$to <- c(
   "BCastaneda@fhi360.org",
   "RLepardo@fhi360.org",
   "JPalo@fhi360.org"
)

icap$mail$for_checking <- gm_mime() %>%
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
   gm_subject(glue("[For Checking] {icap$mail$title}")) %>%
   gm_html_body(
      glue(
         r"(
<p>Good day,</p>
<p>You are receiving this message because you are a member of one of the HIV surveillance systems utilized in generating the ICAP MER Indicators.
<p>This communication is specifically for the monthly indicators and the required counter-checking and validation to be conducted prior to releasing to the ICAP SI Team.</p>
<p>For the staff new to the MER Indicators CC, reach out to your seniors to provide context on what is to be done.</p>
<p>Attached are the files pertaining to the subject above. Kindly conduct the necessary checking. Final release is slated to be before COB today, May 10.</p>
{gmail_sig()}
         )"
      )
   ) %>%
   gm_attach_file(file.path(icap$xlsx$dir, "ICAP QR Indicators (2022.Q3).xlsx"))

icap$mail$for_approval <- gm_mime() %>%
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
   gm_subject(glue("[For Approval] {icap$mail$title}")) %>%
   gm_html_body(
      glue(
         r"(
<p>Hi Sir Noel & Ate Ja,</p>
<p>Attached here is the flat file for the aforementioned indicators set to be released to the ICAP SI Team. All indicators were prepared by myself.</p>
<p>Once you provide approval, I will release ASAP to the ICAP SI Team.</p>
{gmail_sig()}
         )"
      )
   ) %>%
   gm_attach_file(file.path(icap$xlsx$dir, "ICAP QR Indicators (2022.Q3).xlsx"))

icap$mail$for_release <- gm_mime() %>%
   gm_to(
      icap$mail$to
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
   gm_subject(glue("[Data Release] {icap$mail$title}")) %>%
   gm_html_body(
      glue(
         r"(
<p>Hi Ate Betts & Kuya Chard,</p>
<p>Attached here are the QR MER Indicators for ICAP Sites as of Jun 2022. All data utilized was extracted from OHASIS as of 8AM today.</p>
<p>Should you have any questions or clarifications, please don't hesitate to let me know.</p>
{gmail_sig()}
         )"
      )
   ) %>%
   gm_attach_file(file.path(icap$xlsx$dir, "ICAP QR Indicators (2022.Q3).xlsx"))
