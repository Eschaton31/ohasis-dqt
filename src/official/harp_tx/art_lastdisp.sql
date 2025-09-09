select *
from (select data.central_id,
             data.rec_id,
             data.visit_date,
             row_number() over (partition by central_id order by visit_date desc) as visit_num
      from (
               select coalesce(reg.central_id, rec.patient_id) as central_id,
                      rec.*
               from ohasis_warehouse.form_art_bc as rec
                        left join ohasis_lake.id_registry as reg on rec.patient_id = reg.patient_id
               where medicine_summary is not NULL and date(visit_date) <= ?
           ) as data) as artstart
where visit_num = 1;