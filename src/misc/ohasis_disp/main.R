facis <- ohasis$ref_faci %>%
   filter(FACI_NHSSS_REG == "6")
min <- "2024-01-01"
max <- "2024-04-30"

conn  <- ohasis$conn("db")
forms <- QB$new(conn)
forms$from("ohasis_interim.px_record AS rec")
forms$join("ohasis_interim.px_medicine AS meds", "rec.REC_ID", "=", "meds.REC_ID")
forms$leftJoin("ohasis_interim.registry", "rec.PATIENT_ID", "=", "registry.PATIENT_ID")
forms$leftJoin("ohasis_interim.px_faci AS form_bc", "rec.REC_ID", "=", "form_bc.REC_ID")
forms$leftJoin("ohasis_interim.px_info AS info", "rec.REC_ID", "=", "info.REC_ID")
forms$leftJoin("ohasis_interim.px_name AS name", "rec.REC_ID", "=", "name.REC_ID")
forms$leftJoin("ohasis_interim.inventory_product AS ref_meds", "meds.MEDICINE", "=", "ref_meds.ITEM")
forms$whereNull("rec.DELETED_AT")
forms$where("rec.DISEASE", "=", "101000")
forms$where("rec.MODULE", "=", "3")
forms$where("form_bc.SERVICE_TYPE", "=", "101201")
forms$whereIn("rec.FACI_ID", facis$FACI_ID)
forms$whereBetween("rec.RECORD_DATE", c("2024-01-01", "2024-03-01"))
forms$select(
   "rec.REC_ID",
   "registry.CENTRAL_ID",
   "rec.PATIENT_ID",
   "rec.CREATED_BY",
   "rec.CREATED_AT",
   "rec.RECORD_DATE",
   "info.CONFIRMATORY_CODE",
   "info.UIC",
   "info.PHILHEALTH_NO",
   "info.SEX",
   "info.BIRTHDATE",
   "info.PATIENT_CODE",
   "name.FIRST",
   "name.MIDDLE",
   "name.LAST",
   "name.SUFFIX",
   "ref_meds.NAME AS MEDICINE",
   "ref_meds.SHORT AS SHORT",
   "meds.BATCH_NUM",
   "meds.UNIT_BASIS",
   "meds.PER_DAY",
   "meds.DISP_TOTAL",
   "meds.MEDICINE_LEFT",
   "meds.MEDICINE_MISSED",
   "meds.DISP_DATE",
   "meds.NEXT_DATE",
   "meds.DISP_BY",
   "meds.FACI_ID AS DISP_FACI",
   "meds.SUB_FACI_ID AS DISP_SUB_FACI",
   "rec.FACI_ID",
   "rec.SUB_FACI_ID",
   "form_bc.FACI_ID AS SERVICE_FACI",
   "form_bc.SUB_FACI_ID AS SERVICE_SUB_FACI"
)
disp <- forms$get()

all_disp <- disp %>%
   mutate(
      # tag those without form faci
      use_record_faci = if_else(
         condition = is.na(SERVICE_FACI),
         true      = 1,
         false     = 0
      ),
      SERVICE_FACI    = if_else(
         condition = use_record_faci == 1,
         true      = FACI_ID,
         false     = SERVICE_FACI
      ),
      .before         = 1
   ) %>%
   ohasis$get_staff(c(CREATED = "CREATED_BY")) %>%
   ohasis$get_staff(c(DISPENSED = "DISP_BY")) %>%
   ohasis$get_faci(
      list(FACI_VISIT = c("FACI_ID", "SUB_FACI_ID")),
      "name",
   ) %>%
   ohasis$get_faci(
      list(FACI_ART = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
      "name",
   ) %>%
   ohasis$get_faci(
      list(FACI_DISP = c("DISP_FACI", "DISP_SUB_FACI")),
      "name",
   ) %>%
   mutate(
      CENTRAL_ID = coalesce(CENTRAL_ID, PATIENT_ID),
      SEX        = case_when(
         SEX == 1 ~ "Male",
         SEX == 2 ~ "Female",
      ),
      UNIT_BASIS = case_when(
         UNIT_BASIS == 1 ~ "Bottles",
         UNIT_BASIS == 2 ~ "Pills/mL",
      ),
   ) %>%
   mutate_at(
      .vars = vars(CREATED_AT, DISP_DATE, NEXT_DATE),
      ~as.Date(.)
   ) %>%
   select(
      `Record ID`             = REC_ID,
      `Central ID`            = CENTRAL_ID,
      `Patient ID`            = PATIENT_ID,
      `Encoded on:`           = CREATED_AT,
      `Encoded by:`           = CREATED,
      `Facility Visited`      = FACI_VISIT,
      `Visit Date`            = RECORD_DATE,
      `ART Facility`          = FACI_ART,
      `HIV Confirmatory Code` = CONFIRMATORY_CODE,
      `UIC`                   = UIC,
      # `PhilHealth No.`              = PHILHEALTH_NO,
      Sex                     = SEX,
      `Birth Date`            = BIRTHDATE,
      `Patient Code`          = PATIENT_CODE,
      # `First Name`                  = FIRST,
      # `Middle Name`                 = MIDDLE,
      # `Last Name`                   = LAST,
      # `Suffix (Jr., Sr., III, etc)` = SUFFIX,
      `Dispensing Facility`   = FACI_DISP,
      `Medicine`              = MEDICINE,
      `Medicine (Short-hand)` = SHORT,
      `Batch Code / Lot No.`  = BATCH_NUM,
      `Dispensing Type`       = UNIT_BASIS,
      `Dosage per day`        = PER_DAY,
      `Total Dispensed`       = DISP_TOTAL,
      `Leftover pills/mL`     = MEDICINE_LEFT,
      `Missed pills/mL`       = MEDICINE_MISSED,
      `Date Dispensed`        = DISP_DATE,
      `Next scheduled visit`  = NEXT_DATE,
      `Dispensed by:`         = DISPENSED,
   )

write_flat_file(list(Dispensing = all_disp), "H:/20240515_arv-dispensing_reg6-202401-202404.xlsx")
