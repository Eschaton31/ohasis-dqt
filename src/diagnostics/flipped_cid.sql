SELECT *
FROM ohasis_interim.registry
WHERE CENTRAL_ID IN (SELECT PATIENT_ID FROM ohasis_interim.registry WHERE CENTRAL_ID <> PATIENT_ID)