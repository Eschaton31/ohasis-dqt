SELECT *
FROM registry
WHERE CENTRAL_ID IN (SELECT PATIENT_ID FROM registry WHERE CENTRAL_ID <> PATIENT_ID)