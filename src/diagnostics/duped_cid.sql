SELECT *
FROM ohasis_interim.registry
WHERE PATIENT_ID IN (SELECT PATIENT_ID FROM ohasis_interim.registry GROUP BY PATIENT_ID HAVING COUNT(*) > 1)