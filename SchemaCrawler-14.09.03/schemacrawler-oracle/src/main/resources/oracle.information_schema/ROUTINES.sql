SELECT /*+ PARALLEL(AUTO) */
  NULL AS ROUTINE_CATALOG,
  PROCEDURES.OWNER AS ROUTINE_SCHEMA,
  PROCEDURES.OBJECT_NAME AS ROUTINE_NAME,
  PROCEDURES.OBJECT_NAME AS SPECIFIC_NAME,
  'SQL' AS ROUTINE_BODY,
  DBMS_METADATA.GET_DDL(OBJECT_TYPE, PROCEDURES.OBJECT_NAME, PROCEDURES.OWNER) 
    AS ROUTINE_DEFINITION
FROM
  ALL_PROCEDURES PROCEDURES
WHERE
  -- REGEXP_LIKE(PROCEDURES.OWNER, '${schemas}')
  PROCEDURES.OWNER = '${schemas}'
  AND PROCEDURES.AUTHID = 'CURRENT_USER'
ORDER BY
  ROUTINE_SCHEMA,
  ROUTINE_NAME
