SELECT COALESCE(id.CENTRAL_ID, form.PATIENT_ID) AS CENTRAL_ID,
       form.*
FROM ohasis_warehouse.form_d AS form
         LEFT JOIN ohasis_warehouse.id_registry AS id USING (PATIENT_ID)
WHERE COALESCE(id.CENTRAL_ID, form.PATIENT_ID) NOT IN (SELECT CENTRAL_ID FROM ohasis_warehouse.harp_dead_old);
