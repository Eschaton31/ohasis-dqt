pw      <- list()
pw$mail <- gm_mime() %>%
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
   gm_subject(glue("[Data Security] Password Update - nhsss@doh.gov.ph")) %>%
   gm_html_body(
      glue(
         r"(
<p>Good morning everyone,</p>
<p>Recently, we've had a number of departures of staff from our unit. As part of our data security policies, the time has come to update the password of our shared/team email.</p>
<p>See attached image for the updated password for the account indicated in the title.</p>
<p>Best,<br>- Bene</p>
{gmail_sig()}
         )"
      )
   ) %>%
   gm_attach_file("C:/Users/johnb/Documents/ShareX/Screenshots/2022.07/2022.07.04/2022.07.04.081912 (Notepad).png")

pw$mail <- gm_mime() %>%
   gm_to(
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
   gm_subject(glue("[Data Security] Password Update - nhsss@doh.gov.ph")) %>%
   gm_html_body(
      glue(
         r"(
<p>Hi Sir, re-attaching the new password for the aforementioned account.</p>
<p>Best,<br>- Bene</p>
{gmail_sig()}
         )"
      )
   ) %>%
   gm_attach_file("C:/Users/johnb/Documents/ShareX/Screenshots/2022.07/2022.07.04/2022.07.04.081912 (Notepad).png")

gm_send_message(pw$mail, thread_id = "181c6983906306ef")