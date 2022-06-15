SELECT *
FROM ohasis_interim.registry
WHERE CENTRAL_ID NOT IN (SELECT PATIENT_ID FROM ohasis_interim.registry)
GROUP BY CENTRAL_ID