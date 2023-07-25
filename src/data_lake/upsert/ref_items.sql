SELECT data.ITEM,
       data.NAME,
       data.SHORT,
       data.TYPICAL_BATCH,
       data.`ORDER`,
       data.CREATED_AT,
       data.UPDATED_AT,
       data.DELETED_AT,
       GREATEST(COALESCE(data.DELETED_AT, 0), COALESCE(data.UPDATED_AT, 0), COALESCE(data.CREATED_AT, 0)) AS SNAPSHOT
FROM ohasis_interim.inventory_product AS data
WHERE ((data.CREATED_AT BETWEEN ? AND ?) OR
       (data.UPDATED_AT BETWEEN ? AND ?) OR
       (data.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: ITEM;
