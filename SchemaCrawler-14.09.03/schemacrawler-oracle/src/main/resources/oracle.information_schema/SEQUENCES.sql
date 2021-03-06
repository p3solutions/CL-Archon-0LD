SELECT /*+ PARALLEL(AUTO) */
  NULL AS SEQUENCE_CATALOG,
  SEQUENCES.SEQUENCE_OWNER AS SEQUENCE_SCHEMA,
  SEQUENCES.SEQUENCE_NAME AS SEQUENCE_NAME,
  SEQUENCES.INCREMENT_BY AS "INCREMENT",
  SEQUENCES.MIN_VALUE AS MINIMUM_VALUE,
  SEQUENCES.MAX_VALUE AS MAXIMUM_VALUE,
  CASE WHEN SEQUENCES.CYCLE_FLAG = 'Y' THEN 'YES' ELSE 'NO' END AS CYCLE_OPTION,
  SEQUENCES.ORDER_FLAG,
  SEQUENCES.CACHE_SIZE,
  SEQUENCES.LAST_NUMBER
FROM
  ALL_SEQUENCES SEQUENCES
WHERE
  -- REGEXP_LIKE(SEQUENCES.SEQUENCE_OWNER, '${schemas}')
  SEQUENCES.SEQUENCE_OWNER = '${schemas}'
ORDER BY
  SEQUENCE_OWNER,
  SEQUENCE_NAME
