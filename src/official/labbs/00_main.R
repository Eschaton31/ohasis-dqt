labbs <- new.env()

# set paths and reporting period
local(envir = labbs, {
   params        <- list()
   params$gdrive <- as_id("17-JYnMzUvTOa59u72FkETmGC8vncD8WZ")
   params$mo     <- input(prompt = "What is the reporting month?", max.char = 2)
   params$mo     <- stri_pad_left(params$mo, width = 2, pad = "0")
   params$yr     <- input(prompt = "What is the reporting year?", max.char = 4)
   params$yr     <- stri_pad_left(params$yr, width = 4, pad = "0")
   params$ym     <- paste0(params$yr, ".", params$mo)
})

# get gdrive configs
local(envir = labbs, {
   # get submissions
   params$gdrive_ym <- drive_ls(params$gdrive, params$ym)$id

   # configs
   config      <- list()
   config$src  <- as_id("1TNxJrnV_yjvvKW3Pb9eNuFfJphyl6CPyxuAOQPDo8sc")
   config$cols <- read_sheet(config$src, "cols")

   paths                <- drive_ls(params$gdrive_ym)
   params$gdrive_submit <- filter(paths, name == "Submissions")$id
   params$gdrive_valid  <- filter(paths, name == "Validations")$id
   rm(paths)
})

# 1) get list of submissions from LaBBS-DQT drive for the quarter
# 2) download files into local directory
files_submit <- drive_ls(labbs$params$gdrive_submit)
invisible(lapply(seq_len(nrow(files_submit)), function(i, drive_files, params) {
   file   <- drive_files[i,]$id
   output <- file.path(Sys.getenv("LABBS"), params$ym, drive_files[i,]$name)
   drive_download(file, output, overwrite = TRUE)
}, files_submit, labbs$params))


# 1) get list of downloaded excel files
files_submit <- dir_ls(file.path(Sys.getenv("LABBS"), labbs$params$ym))

labbs_reports     <- list(
   faci_list = "List of Facilities",
   hiv       = "HIV",
   bbhiv     = "BLOODHIV",
   hepb      = "Hepatitis B",
   bbhepb    = "BLOODHepB",
   hepc      = "Hepatitis C",
   bbhepc    = "BLOODHepC",
   syp       = "Syphilis",
   bbsyp     = "BLOODSyph",
   gono      = "Gonorrhea"
)
labbs$data        <- lapply(seq_len(length(labbs_reports)), function(i, files_submit) {
   sheet_data <- lapply(files_submit, labbs_sheet, names(labbs_reports)[[i]], labbs_reports[[i]])
   sheet_data <- bind_rows(sheet_data)
   return(sheet_data)
}, files_submit)
names(labbs$data) <- names(labbs_reports)
invisible(lapply(files_submit, labbs_sheet, "gono", "Gonorrhea"))
