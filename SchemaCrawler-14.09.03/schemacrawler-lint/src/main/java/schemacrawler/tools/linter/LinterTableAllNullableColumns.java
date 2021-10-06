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
package schemacrawler.tools.linter;


import static java.util.Objects.requireNonNull;

import java.sql.Connection;
import java.util.Collection;

import schemacrawler.schema.Column;
import schemacrawler.schema.Table;
import schemacrawler.schema.View;
import schemacrawler.tools.lint.BaseLinter;

public class LinterTableAllNullableColumns
  extends BaseLinter
{

  @Override
  public String getSummary()
  {
    return "all data columns are nullable";
  }

  @Override
  protected void lint(final Table table, final Connection connection)
  {
    requireNonNull(table, "No table provided");

    if (!(table instanceof View) && hasAllNullableColumns(getColumns(table)))
    {
      addTableLint(table, getSummary());
    }
  }

  private boolean hasAllNullableColumns(final Collection<Column> columns)
  {
    boolean hasAllNullableColumns = true;
    for (final Column column: columns)
    {
      if (!column.isPartOfPrimaryKey() && !column.isNullable())
      {
        hasAllNullableColumns = false;
        break;
      }
    }
    return hasAllNullableColumns;
  }

}
