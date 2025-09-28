dir <- "H:/Documentations/Human Resources/FHI 360/Communications + Internet/202501-202507"

periods  <- c("2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06", "2025-07")

comms <- c()
internet <- c()
for (period in periods) {
   comms <- append(comms, file.path(dir, glue("Title - {period}.pdf")))
   comms <- append(comms, file.path(dir, glue("Smart SOA - {period}.pdf")))
   comms <- append(comms, file.path(dir, glue("Smart Receipt - {period}.pdf")))
   internet <- append(internet, file.path(dir, glue("Title - {period}.pdf")))
   internet <- append(internet, file.path(dir, glue("PLDT SOA - {period}.pdf")))
   internet <- append(internet, file.path(dir, glue("PLDT Receipt - {period}.pdf")))
}
pdftools::pdf_combine(comms, output = file.path(dir, "JPalo - Communications SOA & Receipt 2025-01 to 2025-07.pdf"))
pdftools::pdf_combine(internet, output = file.path(dir, "JPalo - Internet SOA & Receipt 2025-01 to 2025-07.pdf"))

