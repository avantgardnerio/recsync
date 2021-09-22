select CONSTRAINT_NAME,
       ORDINAL_POSITION,
       TABLE_NAME,
       COLUMN_NAME,
       REFERENCED_TABLE_NAME,
       REFERENCED_COLUMN_NAME
from information_schema.key_column_usage
where referenced_table_name is not null
  and TABLE_SCHEMA=?
order by CONSTRAINT_NAME, ORDINAL_POSITION
;