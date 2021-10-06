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
package schemacrawler.utility;


import static java.util.Objects.requireNonNull;
import static sf.util.DatabaseUtility.executeSql;
import static sf.util.DatabaseUtility.executeSqlForLong;
import static sf.util.DatabaseUtility.executeSqlForScalar;
import static sf.util.TemplatingUtility.expandTemplate;
import static sf.util.Utility.isBlank;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import schemacrawler.schema.Column;
import schemacrawler.schema.JavaSqlType.JavaSqlTypeGroup;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.InclusionRule;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import sf.util.StringFormat;

public final class QueryUtility
{

  private static final Logger LOGGER = Logger
    .getLogger(QueryUtility.class.getName());

  public static ResultSet executeAgainstSchema(final Query query,
                                               final Statement statement,
                                               final InclusionRule schemaInclusionRule)
    throws SQLException
  {
    requireNonNull(query, "No query provided");
    query.setQuoteIdentifier(statement.getConnection().getMetaData()
      .getIdentifierQuoteString());
    final String sql = getQuery(query, schemaInclusionRule);
    LOGGER.log(Level.FINE,
               new StringFormat("Executing %s: %n%s", query.getName(), sql));

    // String database = statement.getConnection().getCatalog();
    // System.out.println("DATABASE : = " + database);
    // System.out.println("SQL = " + sql.replace("$database",
    // database));
    // return executeSql(statement, sql.replace("$database", database));
    return executeSql(statement, sql);
  }

  public static ResultSet executeAgainstTable(final Query query,
                                              final Statement statement,
                                              final Table table,
                                              final boolean isAlphabeticalSortForTableColumns)
    throws SQLException
  {
    requireNonNull(query, "No query provided");
    query.setQuoteIdentifier(statement.getConnection().getMetaData()
      .getIdentifierQuoteString());
    final String sql = getQuery(query,
                                table,
                                isAlphabeticalSortForTableColumns);
    LOGGER.log(Level.FINE,
               new StringFormat("Executing %s: %n%s", query.getName(), sql));
    try
    {
      ResultSet rs = executeSql(statement, sql);
      if (rs == null)
      {
        if (query.getName().equalsIgnoreCase("count"))
        {
          return executeSql(statement,
                            "SELECT COUNT(*) FROM " + table.getFullName() + "");
        }
      }
      else
      {
        return rs;
      }
    }
    catch (SQLException e)
    {
      if (query.getName().equalsIgnoreCase("count"))
      {
        return executeSql(statement,
                          "SELECT COUNT(*) FROM " + table.getFullName() + "");
      }
      else
      {
        throw new SQLException(e);
      }
    }
    return null;
  }

  public static long executeForLong(final Query query,
                                    final Connection connection,
                                    final Table table)
    throws SchemaCrawlerException, SQLException
  {
    requireNonNull(query, "No query provided");
    query
      .setQuoteIdentifier(connection.getMetaData().getIdentifierQuoteString());
    final String sql = getQuery(query, table, true);
    LOGGER.log(Level.FINE,
               new StringFormat("Executing %s: %n%s", query.getName(), sql));
    return executeSqlForLong(connection, sql);
  }

  public static Object executeForScalar(final Query query,
                                        final Connection connection)
    throws SchemaCrawlerException, SQLException
  {
    requireNonNull(query, "No query provided");
    query
      .setQuoteIdentifier(connection.getMetaData().getIdentifierQuoteString());
    final String sql = getQuery(query);
    LOGGER.log(Level.FINE,
               new StringFormat("Executing %s: %n%s", query.getName(), sql));
    return executeSqlForScalar(connection, sql);
  }

  public static Object executeForScalar(final Query query,
                                        final Connection connection,
                                        final Table table)
    throws SchemaCrawlerException, SQLException
  {
    requireNonNull(query, "No query provided");
    query
      .setQuoteIdentifier(connection.getMetaData().getIdentifierQuoteString());
    final String sql = getQuery(query, table, true);
    LOGGER.log(Level.FINE,
               new StringFormat("Executing %s: %n%s", query.getName(), sql));
    return executeSqlForScalar(connection, sql);
  }

  private static String getColumnsListAsString(final List<Column> columns,
                                               final boolean omitLargeObjectColumns)
  {
    final StringBuilder buffer = new StringBuilder(1024);
    for (int i = 0; i < columns.size(); i++)
    {
      final Column column = columns.get(i);
      final JavaSqlTypeGroup javaSqlTypeGroup = column.getColumnDataType()
        .getJavaSqlType().getJavaSqlTypeGroup();
      if (!(omitLargeObjectColumns
            && javaSqlTypeGroup == JavaSqlTypeGroup.large_object))
      {
        if (i > 0)
        {
          buffer.append(", ");
        }
        buffer.append(column.getName());
      }
    }
    return buffer.toString();
  }

  private static String getQuery(final Query query)
  {
    return expandTemplate(query.getQuery());
  }

  /**
   * Gets the query with parameters substituted.
   *
   * @param schemaInclusionRule
   *        Schema inclusion rule
   * @return Ready-to-execute query
   */
  private static String getQuery(final Query query,
                                 final InclusionRule schemaInclusionRule)
  {
    final Map<String, String> properties = new HashMap<>();
    if (schemaInclusionRule != null)
    {
      final String schemaInclusionPattern = schemaInclusionRule
        .getInclusionPattern().pattern();
      if (!isBlank(schemaInclusionPattern))
      {
        properties
          .put("schemas",
               query.getQuery()
                 .contains("'${schemas}'")? schemaInclusionPattern
                                          : getQuotedExpressionName(schemaInclusionPattern,
                                                                    query
                                                                      .getQuoteIdentifier()));
      }
    }

    String sql = query.getQuery();
    sql = expandTemplate(sql, properties);
    sql = expandTemplate(sql);
    return sql;
  }

  private static String getQuery(final Query query,
                                 final Table table,
                                 final boolean isAlphabeticalSortForTableColumns)
  {
    final Map<String, String> tableProperties = new HashMap<>();
    if (table != null)
    {
      final NamedObjectSort columnsSort = NamedObjectSort
        .getNamedObjectSort(isAlphabeticalSortForTableColumns);
      final List<Column> columns = table.getColumns();
      Collections.sort(columns, columnsSort);

      if (table.getSchema() != null)
      {
        tableProperties.put("schema",
                            query.getQuery()
                              .contains("'${schemas}'")? table.getSchema().getFullName(): getQuotedExpressionName(table.getSchema().getFullName(), query.getQuoteIdentifier()));
      }
      tableProperties.put("table",
                          getQuotedExpressionName(table.getFullName(),
                                                  query.getQuoteIdentifier()));
      tableProperties.put("tablename",
                          getQuotedExpressionName(table.getName(),
                                                  query.getQuoteIdentifier()));
      tableProperties.put("columns", getColumnsListAsString(columns, false));
      tableProperties.put("orderbycolumns",
                          getColumnsListAsString(columns, true));
      tableProperties.put("tabletype", table.getTableType().toString());
    }

    String sql = query.getQuery();
    sql = expandTemplate(sql, tableProperties);
    sql = expandTemplate(sql);
    return sql;
  }

  private static String getQuotedExpressionName(String text,
                                                String escpaeIdentifier)
  {
    if (text.contains("."))
    {
      StringBuffer sb = new StringBuffer();
      String splits[] = text.split("\\.");
      for (String part: splits)
        sb.append(".")
          .append(checkAndAddQuoteExpression(part, escpaeIdentifier));
      return sb.length() > 0? sb.toString().substring(1): text;
    }
    else
      return checkAndAddQuoteExpression(text, escpaeIdentifier);
  }

  private static String checkAndAddQuoteExpression(String text,
                                                   String escpaeIdentifier)
  {
    if (escpaeIdentifier == null) return text;
    return (text.startsWith(escpaeIdentifier)
            && text.endsWith(escpaeIdentifier))? text
                                               : escpaeIdentifier.concat(text)
                                                 .concat(escpaeIdentifier);
  }

  private QueryUtility()
  {
    // Prevent instantiation
  }

}
