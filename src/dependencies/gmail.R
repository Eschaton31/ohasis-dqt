# generating signature
gmail_sig <- function() {
   glue(r"(
--<br>
<p>Sent from R on {format(Sys.time(), "%a %b %d, %Y %X")}</p>
--<br>
<div dir="ltr">
   <div><b><font face="tahoma, sans-serif" size="4">{Sys.getenv("GMAIL_NAME")}</font></b></div>
   <div>
      <font face="tahoma, sans-serif" style="font-size:x-small"><i>{Sys.getenv("GMAIL_DESIG")}&nbsp;</i>|&nbsp;</font>
      <span style="font-size:x-small;font-family:tahoma,sans-serif">National HIV/AIDS &amp; STI Surveillance &amp; Strategic Information Unit</span>
   </div>
   <div><font face="tahoma, sans-serif" size="1">Epidemiology Bureau | Department of Health</font></div>
   <div><font face="tahoma, sans-serif" size="1">San Lazaro Compound, Rizal Avenue, Sta. Cruz, Manila</font>
   </div>
   <div><font face="tahoma, sans-serif" size="1">Phone: 651-7800 local 2952 | Mobile: {Sys.getenv("GMAIL_MOBILE")}</font>
   </div>
   <div>
      <font face="tahoma, sans-serif" size="1">E-mail:
         <a href="mailto:{Sys.getenv("GMAIL_USER")}" target="_blank">{Sys.getenv("GMAIL_USER")}</a>&nbsp;/
         <a href="mailto:{Sys.getenv("GMAIL_ALT")}" target="_blank">{Sys.getenv("GMAIL_ALT")}</a>
      </font>
   </div>
</div>
)")
}

if (Sys.getenv("GMAIL_KEY") != "") {
   # auth files
   gm_auth_configure(
      key    = Sys.getenv("GMAIL_KEY"),
      secret = Sys.getenv("GMAIL_SECRET"),
   )
   gm_auth(email = Sys.getenv("GMAIL_USER"))
}

# specific to NHSSS unit details
gmail             <- new.env()
gmail$nhsss$head  <- "nspalaypayon@doh.gov.ph"
gmail$nhsss$ss    <- c(
   "jsmartinez@doh.gov.ph",
   "cjtinaja@doh.gov.ph",
   "jmdeliso@doh.gov.ph",
   "macabreros@doh.gov.ph"
)
gmail$nhsss$dqt   <- c(
   "rphaynes@doh.gov.ph",
   "nesaplagio@doh.gov.ph",
   "fgsolquio@doh.gov.ph"
)
gmail$nhsss$dat   <- c(
   "jsmartinez@doh.gov.ph",
   "cjtinaja@doh.gov.ph",
   "kapamittan@doh.gov.ph",
   "roricaflanca.doh@gmail.com",
   "roricaflanca@doh.gov.ph",
   "jmdeliso@doh.gov.ph",
   "macabreros@doh.gov.ph",
   "nesaplagio@doh.gov.ph"
)
gmail$nhsss$ihbss <- c(
   "jsmartinez@doh.gov.ph",
   "cjtinaja@doh.gov.ph",
   "roricaflanca.doh@gmail.com",
   "roricaflanca@doh.gov.ph"
)