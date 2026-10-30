# make variable names stata-safe: valid characters, max 32 chars, unique
stata_names <- function(names) {
   names <- stri_replace_all_regex(names, "[^\\p{L}\\p{N}_]", "_")
   names <- if_else(stri_detect_regex(names, "^[\\p{L}_]"), names, paste0("_", names))
   names <- stri_sub(names, 1, 32)

   for (i in which(duplicated(names))) {
      k <- 1
      repeat {
         suffix    <- paste0("_", k)
         candidate <- paste0(stri_sub(names[i], 1, 32 - nchar(suffix)), suffix)
         if (!candidate %in% names) break
         k <- k + 1
      }
      names[i] <- candidate
   }

   return(names)
}

# format a single column for stata export
format_stata_col <- function(x) {
   # double is exact up to 2^53; as.integer() overflows to NA past 2^31
   if (bit64::is.integer64(x))
      x <- as.numeric(x)

   if (is.character(x)) {
      # drop escape backslashes (\' \") and any stray backslashes
      if (any(stri_detect_fixed(x, "\\"), na.rm = TRUE))
         x <- stri_replace_all_fixed(x, "\\", "")

      # stata str widths are in bytes; min width of 1 for empty / all-NA columns
      width                   <- suppressWarnings(max(nchar(x, type = "bytes"), na.rm = TRUE))
      attr(x, "format.stata") <- paste0("%-", max(1L, width), "s")
   } else if (inherits(x, "Date")) {
      attr(x, "format.stata") <- "%tdCCYY-NN-DD"
   } else if (inherits(x, "POSIXct")) {
      attr(x, "format.stata") <- "%tcCCYY-NN-DD_HH:MM:SS"
   } else if (is.double(x) && !is.object(x) && !all(is.na(x))) {
      # whole numbers within integer range are stored as long instead of double
      range <- range(x, na.rm = TRUE)
      if (range[1] >= -.Machine$integer.max && range[2] <= .Machine$integer.max && all(x == trunc(x), na.rm = TRUE))
         storage.mode(x) <- "integer"
   }

   return(x)
}

# format stata exports
format_stata <- function(data) {
   data[]       <- lapply(data, format_stata_col)
   names(data)  <- stata_names(names(data))

   return(data)
}

# compress one or more .dta files in a single stata session
compress_stata <- function(files) {
   stata_path <- stri_replace_all_fixed(Sys.getenv("STATA_PATH"), '"', "")
   if (stata_path == "")
      return(invisible(NULL))

   files   <- normalizePath(files, winslash = "/", mustWork = TRUE)
   run_dir <- tempfile("stata-")
   dir.create(run_dir)
   on.exit(unlink(run_dir, recursive = TRUE), add = TRUE)

   status_file <- file.path(run_dir, "status.txt")
   do_file     <- file.path(run_dir, "compress.do")

   compress_block <- function(file) glue(.open = "<<", .close = ">>", r"(
cap noi {
   u "<<file>>", clear

   cap ds, has(type string)
   if !_rc {
      foreach var in `r(varlist)' {
         loc type : type `var'
         loc len = substr("`type'", 4, 1000)

         cap form `var' %-`len's
      }
   }

   cap ds, has(type numeric)
   if !_rc {
      foreach var in `r(varlist)' {
         loc fmt : format `var'
         if regexm("`fmt'", "^%-?tc") form `var' %tcCCYY-NN-DD_HH:MM:SS
         else if regexm("`fmt'", "^%-?tC") form `var' %tCCCYY-NN-DD_HH:MM:SS
         else if strpos("`var'", "date") & !regexm("`fmt'", "^%-?t[^d]") form `var' %tdCCYY-NN-DD
      }
   }

   compress
   sa "<<file>>", replace
}
file write fh "`=_rc'" _n
)")

   stata_cmd <- c(
      glue('file open fh using "{status_file}", write text replace'),
      vapply(files, compress_block, character(1)),
      "file close fh"
   )
   writeLines(stata_cmd, do_file)

   # batch mode writes its log to the working directory, keep it in run_dir
   old_wd <- setwd(run_dir)
   on.exit(setwd(old_wd), add = TRUE, after = FALSE)
   system2(stata_path, c("/e", "do", shQuote(do_file, type = "cmd")), wait = TRUE)

   status <- if (file.exists(status_file)) suppressWarnings(as.integer(readLines(status_file))) else integer()
   failed <- files[is.na(status[seq_along(files)]) | status[seq_along(files)] != 0]
   if (length(failed) > 0)
      warning("Stata compress failed for:\n", paste0("  ", failed, collapse = "\n"), call. = FALSE)

   return(invisible(files))
}
