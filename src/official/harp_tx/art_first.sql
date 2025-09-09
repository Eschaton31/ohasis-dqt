select *
from (select data.central_id,
             data.rec_id,
             data.visit_date,
             row_number() over (partition by central_id order by visit_date) as visit_num
      from (
               select case
                          when reg.central_id is NULL then rec.patient_id
                          when reg.central_id is not NULL then reg.central_id
                          end as central_id,
                      rec.*
               from ohasis_warehouse.form_art_bc rec
                        left join ohasis_lake.id_registry reg on rec.patient_id = reg.patient_id
               where art_record = 'ART' and date(visit_date) <= ?
           ) as data) as artstart
where visit_num = 1;