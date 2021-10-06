/*
========================================================================
SchemaCrawler
http://www.schemacrawler.com
Copyright (c) 2000-2016, Sualeh Fatehi <sualeh@hotmail.com>.
All rights reserved.
------------------------------------------------------------------------

SchemaCrawler is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

SchemaCrawler and the accompanying materials are made available under
the terms of the Eclipse Public License v1.0, GNU General Public License
v3 or GNU Lesser General Public License v3.

You may elect to redistribute this code under any of these licenses.

The Eclipse Public License is available at:
http://www.eclipse.org/legal/epl-v10.html

The GNU General Public License v3 and the GNU Lesser General Public
License v3 are available at:
http://www.gnu.org/licenses/

========================================================================
*/
package schemacrawler.schemacrawler;


public enum InformationSchemaKey
{

 ADDITIONAL_COLUMN_ATTRIBUTES("select.ADDITIONAL_COLUMN_ATTRIBUTES"),
 ADDITIONAL_TABLE_ATTRIBUTES("select.ADDITIONAL_TABLE_ATTRIBUTES"),
 CONSTRAINT_COLUMN_USAGE("select.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE"),
 EXT_FOREIGN_KEYS("select.INFORMATION_SCHEMA.EXT_FOREIGN_KEYS"),
 EXT_HIDDEN_TABLE_COLUMNS("select.INFORMATION_SCHEMA.EXT_HIDDEN_TABLE_COLUMNS"),
 EXT_INDEXES("select.INFORMATION_SCHEMA.EXT_INDEXES"),
 EXT_INDEX_COLUMNS("select.INFORMATION_SCHEMA.EXT_INDEX_COLUMNS"),
 EXT_PRIMARY_KEYS("select.INFORMATION_SCHEMA.EXT_PRIMARY_KEYS"),
 EXT_SYNONYMS("select.INFORMATION_SCHEMA.EXT_SYNONYMS"),
 EXT_TABLES("select.INFORMATION_SCHEMA.EXT_TABLES"),
 EXT_TABLE_CONSTRAINTS("select.INFORMATION_SCHEMA.EXT_TABLE_CONSTRAINTS"),
 FOREIGN_KEYS("select.DATABASE_METADATA.FOREIGN_KEYS"),
 INDEXES("select.DATABASE_METADATA.INDEXES"),
 OVERRIDE_TYPE_INFO("select.OVERRIDE_TYPE_INFO"),
 PRIMARY_KEYS("select.DATABASE_METADATA.PRIMARY_KEYS"),
 ROUTINES("select.INFORMATION_SCHEMA.ROUTINES"),
 SCHEMATA("select.INFORMATION_SCHEMA.SCHEMATA"),
 SEQUENCES("select.INFORMATION_SCHEMA.SEQUENCES"),
 TABLE_COLUMNS("select.DATABASE_METADATA.TABLE_COLUMNS"),
 TABLE_CONSTRAINTS("select.INFORMATION_SCHEMA.TABLE_CONSTRAINTS"),
 TRIGGERS("select.INFORMATION_SCHEMA.TRIGGERS"),
 VIEWS("select.INFORMATION_SCHEMA.VIEWS"),;

  private final String lookupKey;

  private InformationSchemaKey(final String lookupKey)
  {
    this.lookupKey = lookupKey;
  }

  public String getLookupKey()
  {
    return lookupKey;
  }

  public String getResource()
  {
    return name() + ".sql";
  }

}
