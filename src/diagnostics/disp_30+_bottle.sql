SELECT *
FROM ohasis_interim.px_medicine
WHERE DISP_TOTAL >= 30
  AND UNIT_BASIS = 1;