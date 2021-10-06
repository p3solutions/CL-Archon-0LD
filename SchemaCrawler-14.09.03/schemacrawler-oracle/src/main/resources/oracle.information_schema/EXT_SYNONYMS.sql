SELECT /*+ PARALLEL(AUTO) */
  NULL AS SYNONYM_CATALOG,
  SYNONYMS.OWNER AS SYNONYM_SCHEMA,
  SYNONYMS.SYNONYM_NAME,
  NULL AS REFERENCED_OBJECT_CATALOG,
  SYNONYMS.TABLE_OWNER AS REFERENCED_OBJECT_SCHEMA,
  SYNONYMS.TABLE_NAME AS REFERENCED_OBJECT_NAME
FROM
  ALL_SYNONYMS SYNONYMS
WHERE
  -- REGEXP_LIKE(SYNONYMS.OWNER, '${schemas}')
  SYNONYMS.OWNER = '${schemas}'
ORDER BY
  SYNONYM_SCHEMA,
  SYNONYM_NAME
