##  Primary Controller for the IHBSS 2022 Linkage ------------------------------

if (!exists("ihbss"))
   ihbss <- new.env()

ihbss$`2022` <- new.env()

# setup
ruODK::ru_setup(
   svc     = odk_test,
   tz      = "Asia/Hong_Kong",
   verbose = TRUE
)

# list projects & forms
ihbss$`2022`$odk$projects    <- ruODK::project_list()
ihbss$`2022`$odk$forms       <- ruODK::form_list(pid = 7)
ihbss$`2022`$odk$submissions <- ruODK::submission_list(
   pid = 7,
   fid = ihbss$`2022`$odk$forms$fid
)
ihbss$`2022`$odk$data        <- ruODK::odata_submission_get(
   pid      = 7,
   fid      = ihbss$`2022`$odk$forms$fid,
   download = FALSE
)

ruODK::user_list()