patient <- plumber::pr("C:/nhsss/projects/ohasis-dqt/src/api/patient.R")
patient <- plumber::pr("C:/nhsss/projects/ohasis-dqt/src/api/patient.R")
plumber::pr("C:/nhsss/projects/ohasis-dqt/src/api/auth.R") %>%
   plumber::pr_mount("/patient", patient) %>%
   plumber::pr_run(port = 8989)
