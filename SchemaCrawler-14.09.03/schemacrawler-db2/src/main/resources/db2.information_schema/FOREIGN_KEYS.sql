SELECT 
  FK.PKTABLE_CAT,
  FK.PKTABLE_SCHEM,
  FK.PKTABLE_NAME,
  FK.PKCOLUMN_NAME,
  FK.FKTABLE_CAT,
  FK.FKTABLE_SCHEM,
  FK.FKTABLE_NAME,
  FK.FKCOLUMN_NAME,
  FK.KEY_SEQ,
  FK.UPDATE_RULE,
  FK.DELETE_RULE,
  FK.FK_NAME,
  FK.PK_NAME,
  FK.DEFERRABILITY,
  FK.UNIQUE_OR_PRIMARY
FROM
  SYSIBM.SQLFOREIGNKEYS FK
WITH UR
