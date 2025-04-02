dir <- "H:/Documentations/Human Resources/FHI 360/Communications + Internet/202412-202501"
comms <- c(
   file.path(dir, "Title - 2024-12.pdf"),
   file.path(dir, "Smart SOA - 2024-12.pdf"),
   file.path(dir, "Smart Receipt - 2024-12.pdf")
)
internet <- c(
   file.path(dir, "Title - 2024-07.pdf"),
   file.path(dir, "PLDT SOA - 2024-07.pdf"),
   file.path(dir, "PLDT Receipt - 2024-07.pdf"),
   file.path(dir, "Title - 2024-08.pdf"),
   file.path(dir, "PLDT SOA - 2024-08.pdf"),
   file.path(dir, "PLDT Receipt - 2024-08.pdf"),
   file.path(dir, "Title - 2024-09.pdf"),
   file.path(dir, "PLDT SOA - 2024-09.pdf"),
   file.path(dir, "PLDT Receipt - 2024-09.pdf"),
   file.path(dir, "Title - 2024-10.pdf"),
   file.path(dir, "PLDT SOA - 2024-10.pdf"),
   file.path(dir, "PLDT Receipt - 2024-10.pdf"),
   file.path(dir, "Title - 2024-11.pdf"),
   file.path(dir, "PLDT SOA - 2024-11.pdf"),
   file.path(dir, "PLDT Receipt - 2024-11.pdf"),
   file.path(dir, "Title - 2024-12.pdf"),
   file.path(dir, "PLDT SOA - 2024-12.pdf"),
   file.path(dir, "PLDT Receipt - 2024-12.pdf")
)
pdftools::pdf_combine(comms, output = file.path(dir, "JPalo - Communications SOA & Receipt 2024-12.pdf"))
pdftools::pdf_combine(internet, output = file.path(dir, "JPalo - Internet SOA & Receipt 2024-07 to 2024-12.pdf"))

