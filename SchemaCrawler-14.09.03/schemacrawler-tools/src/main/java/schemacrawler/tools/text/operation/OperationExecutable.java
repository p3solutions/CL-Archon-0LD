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

package schemacrawler.tools.text.operation;

import static schemacrawler.utility.QueryUtility.executeAgainstTable;
import static sf.util.DatabaseUtility.createStatement;
import static sf.util.DatabaseUtility.executeSql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import schemacrawler.schema.Catalog;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.tools.executable.BaseStagedExecutable;
import schemacrawler.tools.options.TextOutputFormat;
import schemacrawler.tools.traversal.DataTraversalHandler;
import schemacrawler.utility.NamedObjectSort;
import schemacrawler.utility.Query;

/**
 * Basic SchemaCrawler executor.
 *
 * @author Sualeh Fatehi
 */
public final class OperationExecutable extends BaseStagedExecutable {

	private OperationOptions operationOptions;

	public OperationExecutable(final String command) {
		super(command);
	}

	public String executeCountOn(final String queryStr, final Connection connection) throws Exception {
		final Query query = new Query("Count", queryStr);
		try (final Statement statement = createStatement(connection);) {
			final String sql = query.getQuery();
			try (final ResultSet results = executeSql(statement, sql, true);) {
				results.next();
				return results.getObject(1) + "";
			} catch (Exception e) {
				return "0";
			}
		}
	}

	@Override
	public void executeOn(final Catalog catalog, final Connection connection) throws Exception {
		loadOperationOptions();

		final DataTraversalHandler handler = getDataTraversalHandler();
		final Query query = getQuery();

		try (final Statement statement = createStatement(connection);) {

			handler.begin();

			handler.handleInfoStart();
			handler.handle(catalog.getSchemaCrawlerInfo());
			handler.handle(catalog.getDatabaseInfo());
			handler.handle(catalog.getJdbcDriverInfo());
			handler.handleInfoEnd();

			if (query.getName().equalsIgnoreCase("quickdump") || query.getName().equalsIgnoreCase("dump")) {
				boolean flag = false;
				if (query.isQueryOver()) {
					for (final Table table : getSortedTables(catalog)) {

						// System.out.println("TABLE NAME : " +
						// table.getFullName());
						// System.out.println("QUERY : " + query);

						if (flag)
							break;
						if (operationOptions.getTableNameQuickDump() != null) {
							if (!operationOptions.getTableNameQuickDump().equals(table.getFullName()))
								continue;
							flag = true;
						}
						final boolean isAlphabeticalSortForTableColumns = operationOptions
								.isAlphabeticalSortForTableColumns();
						try (final ResultSet results = executeAgainstTable(query, statement, table,
								isAlphabeticalSortForTableColumns);) {
							handler.handleData(table, results);
						}
					}
				}
			} else if (query.isQueryOver()) {
				for (final Table table : getSortedTables(catalog)) {
					final boolean isAlphabeticalSortForTableColumns = operationOptions
							.isAlphabeticalSortForTableColumns();
					try (final ResultSet results = executeAgainstTable(query, statement, table,
							isAlphabeticalSortForTableColumns);) {
						handler.handleData(table, results);
					}
				}
			} else {
				final String sql = query.getQuery();
				try (final ResultSet results = executeSql(statement, sql, true);) {
					handler.handleData(query, results);
				}
			}

			handler.end();

		} catch (final SQLException e) {
			throw new SchemaCrawlerException("Cannot perform operation", e);
		}
	}

	public final OperationOptions getOperationOptions() {
		loadOperationOptions();
		return operationOptions;
	}

	public final void setOperationOptions(final OperationOptions operationOptions) {
		this.operationOptions = operationOptions;
	}

	private DataTraversalHandler getDataTraversalHandler() throws SchemaCrawlerException {
		final Operation operation = getOperation();

		final OperationOptions operationOptions = getOperationOptions();
		final DataTraversalHandler formatter;
		final TextOutputFormat outputFormat = TextOutputFormat.valueOfFromString(outputOptions.getOutputFormatValue());
		if (outputFormat == TextOutputFormat.xml) {
			formatter = new DataXMLFormatter(operation, operationOptions, outputOptions);
		} else if (outputFormat == TextOutputFormat.arxml) {
			formatter = new DataXMLFormatter(operation, operationOptions, outputOptions);
		} else if (outputFormat == TextOutputFormat.file) {
			formatter = new DataFileFormatter(operation, operationOptions, outputOptions);
		} else if (outputFormat == TextOutputFormat.json) {
			formatter = new DataJsonFormatter(operation, operationOptions, outputOptions);
		} else {
			formatter = new DataTextFormatter(operation, operationOptions, outputOptions);
		}
		return formatter;
	}

	/**
	 * Determine the operation, or whether this command is a query.
	 */
	private Operation getOperation() {
		Operation operation = null;
		try {
			operation = Operation.valueOf(command);
		} catch (final IllegalArgumentException | NullPointerException e) {
			operation = null;
		}
		return operation;
	}

	private Query getQuery() {
		final Operation operation = getOperation();
		final Query query;
		if (operation == null) {
			final String queryName = command;
			final String queryString;
			if (additionalConfiguration != null) {
				queryString = additionalConfiguration.get(queryName);
			} else {
				queryString = null;
			}
			query = new Query(queryName, queryString);
		} else {
			query = operation.getQuery();
		}

		return query;
	}

	private List<? extends Table> getSortedTables(final Catalog catalog) {
		final List<? extends Table> tables = new ArrayList<>(catalog.getTables());
		Collections.sort(tables,
				NamedObjectSort.getNamedObjectSort(getOperationOptions().isAlphabeticalSortForTables()));
		return tables;
	}

	private void loadOperationOptions() {
		if (operationOptions == null) {
			operationOptions = new OperationOptionsBuilder().fromConfig(additionalConfiguration).toOptions();
		}
	}

}
