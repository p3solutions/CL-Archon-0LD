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
import static schemacrawler.utility.QueryUtility.executeForScalar;
import static sf.util.Utility.isBlank;

import java.sql.Connection;

import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.Config;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.tools.lint.BaseLinter;
import schemacrawler.utility.Query;

public class LinterTableSql
  extends BaseLinter
{

  private String message;
  private String sql;

  @Override
  public String getSummary()
  {
    return message;
  }

  @Override
  protected void configure(final Config config)
  {
    requireNonNull(config, "No configuration provided");

    message = config.getStringValue("message", null);
    if (isBlank(message))
    {
      throw new IllegalArgumentException("No message provided");
    }

    sql = config.getStringValue("sql", null);
    if (isBlank(sql))
    {
      throw new IllegalArgumentException("No SQL provided");
    }
  }

  @Override
  protected void lint(final Table table, final Connection connection)
    throws SchemaCrawlerException
  {
    if (isBlank(sql))
    {
      return;
    }

    requireNonNull(table, "No table provided");
    requireNonNull(connection, "No connection provided");

    final Query query = new Query(message, sql);
    final Object queryResult = executeForScalar(query, connection, table);
    if (queryResult != null)
    {
      addTableLint(table, getSummary() + " " + queryResult);
    }
  }

}
