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

import static java.util.Objects.requireNonNull;
import static sf.util.Utility.readFully;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import schemacrawler.crawl.SchemaCrawler;
import schemacrawler.schema.ResultsColumn;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.tools.text.utility.BinaryData;

/**
 * Text formatting of data.
 *
 * @author Sualeh Fatehi
 */
final class DataResultSet {

	private static final Logger LOGGER = Logger.getLogger(DataResultSet.class.getName());

	private final ResultSet rows;

	public ResultSet getRows() {
		return rows;
	}

	private final List<ResultsColumn> resultsColumns;
	private final boolean showLobs;
	private final boolean showDateTime;

	private String[] columnName;
	private String[] columnType;
	private int[] columnTypesJava;
	private int[] columnTypesJavaOrig;
	LinkedHashMap<String, Object[]> dataMap;

	private List<Integer> sortedOrderForOracleFix;

	public DataResultSet(final ResultSet rows, final boolean showLobs, final boolean showDateTime)
			throws SchemaCrawlerException, SQLException {
		this.rows = requireNonNull(rows, "Cannot use null results");
		this.showLobs = showLobs;
		this.showDateTime = showDateTime;
		resultsColumns = SchemaCrawler.getResultColumns(rows).getColumns();
	}

	public String[] getColumnNames() {
		final int columnCount = resultsColumns.size();
		final String[] columnNames = new String[columnCount];
		for (int i = 0; i < columnCount; i++) {
			columnNames[i] = resultsColumns.get(i).getName();
		}
		return columnNames;
	}

	public List<Integer> getSortedOrderForOracleFix() {
		return sortedOrderForOracleFix;
	}

	public void setSortedOrderForOracleFix(List<Integer> sortedOrderForOracleFix) {
		this.sortedOrderForOracleFix = sortedOrderForOracleFix;
	}

	public static String getTextFormatted(String string) {
		string = string.trim().replaceAll("[^_^\\p{Alnum}.]", "_").replace("^", "_").replaceAll("\\s+", "_");
		string = ((string.startsWith("_") && string.endsWith("_") && string.length() > 2)
				? string.substring(1).substring(0, string.length() - 2)
				: string);
		return string.length() > 0 ? ((string.charAt(0) >= '0' && string.charAt(0) <= '9') ? "_" : "") + string
				: string;
	}

	public static String getPriliminaryTextFormatted(String string) {
		String modstring = string.trim().replaceAll("[^_^\\p{Alnum}.]", "_").replace("^", "_").replaceAll("\\s+", "_");
		if (string.equals(modstring))
			return string;
		else
			return "_" + string + "_";

	}

	public void computeColumnInfo() {
		dataMap = new LinkedHashMap<String, Object[]>();
		sortedOrderForOracleFix = new ArrayList<>();
		final int columnCount = resultsColumns.size();
		columnName = new String[columnCount];
		columnType = new String[columnCount];
		columnTypesJava = new int[columnCount];
		columnTypesJavaOrig = new int[columnCount];
		for (int i = 0; i < columnCount; i++) {
			columnName[i] = getPriliminaryTextFormatted(resultsColumns.get(i).getLabel());
			columnType[i] = resultsColumns.get(i).getType().getName().toUpperCase();
			columnTypesJavaOrig[i] = resultsColumns.get(i).getColumnDataType().getJavaSqlType().getJavaSqlType();
			if (columnTypesJava[i] == Types.LONGNVARCHAR || columnTypesJavaOrig[i] == Types.LONGVARCHAR)
				sortedOrderForOracleFix.add(0, i);
			else
				sortedOrderForOracleFix.add(i);

			if (showLobs) {
				if (columnTypesJavaOrig[i] == Types.CLOB || columnTypesJavaOrig[i] == Types.NCLOB
						|| columnTypesJavaOrig[i] == Types.LONGVARCHAR
						|| columnTypesJavaOrig[i] == Types.LONGNVARCHAR) {
					columnTypesJava[i] = 1;
				} else if (columnTypesJavaOrig[i] == Types.BLOB || columnTypesJavaOrig[i] == Types.LONGVARBINARY
						|| columnTypesJavaOrig[i] == Types.VARBINARY) {
					if (resultsColumns.get(i).getColumnDataType().getName().equalsIgnoreCase("VARCHAR FOR BIT DATA")) {
						columnTypesJava[i] = 0;
					} else {
						columnTypesJava[i] = 2;
					}
				} else
					columnTypesJava[i] = 0;
			} else
				columnTypesJava[i] = 0;
			// System.out.println(columnName[i] + "-----" + columnType[i]);
			dataMap.put(getTextFormatted(columnName[i].toUpperCase()),
					new Object[] { columnType[i], columnTypesJava[i],
							resultsColumns.get(i).getColumnDataType().getPrecision(),
							resultsColumns.get(i).getColumnDataType().getMinimumScale() });
		}

	}

	public String[] getColumnName() {
		return columnName;
	}

	public String[] getColumnType() {
		return columnType;
	}

	public int[] getColumnTypeJava() {
		return columnTypesJava;
	}

	public int[] getColumnTypeJavaOrig() {
		return columnTypesJavaOrig;
	}

	public boolean next() throws SQLException {
		return rows.next();
	}

	public List<Object> row() throws SQLException {
		final int columnCount = resultsColumns.size();
		final List<Object> currentRow = new ArrayList<>(columnCount);
		for (int i = 0; i < columnCount; i++) {
			currentRow.add(getColumnData(i));
		}
		return currentRow;
	}

	public List<Object> rowXml() throws SQLException {
		final int columnCount = resultsColumns.size();
		final List<Object> currentRow = new ArrayList<>(columnCount);
		Map<Integer, Object> rowMap = new TreeMap<>();
		for (int i = 0; i < columnCount; i++) {
			rowMap.put(sortedOrderForOracleFix.get(i), getColumnData(sortedOrderForOracleFix.get(i)));
		}
		for (Entry<Integer, Object> entry : rowMap.entrySet()) {
			currentRow.add(entry.getValue());
		}
		return currentRow;
	}

	public int width() {
		return resultsColumns.size();
	}

	private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private final SimpleDateFormat dateOnlyFormat = new SimpleDateFormat("yyyy-MM-dd");

	DecimalFormat formatter;

	private Object getColumnData(final int i) throws SQLException {
		final int javaSqlType = resultsColumns.get(i).getColumnDataType().getJavaSqlType().getJavaSqlType();
		// System.out.println(i + ". " + resultsColumns.get(i).getName() + "-" +
		// javaSqlType);
		// System.out.println(javaSqlType + " --- " +
		// resultsColumns.get(i).getType().getName().toUpperCase() + " --- "
		// + ((ResultsColumn)
		// this.resultsColumns.get(i)).getColumnDataType().getName()
		// + " --- "
		// + (this.rows.getObject(i + 1) != null ? this.rows.getObject(i +
		// 1).toString()
		// : "null"));

		// if (this.rows.getObject(i + 1) != null)
		// this.rows.getObject(i + 1).toString();
		Object columnData;
		if (javaSqlType == Types.SQLXML) {
			SQLXML xmlVal = rows.getSQLXML(i + 1);
			String xml = xmlVal.getString();
			if (rows.wasNull() || xml == null) {
				columnData = null;
			} else {
				columnData = (xml);
			}
		} else if (javaSqlType == Types.CLOB) {
			final Clob clob = rows.getClob(i + 1);
			if (rows.wasNull() || clob == null) {
				columnData = null;
			} else {
				columnData = readClob(clob, null);
			}
		} else if (javaSqlType == Types.NCLOB) {
			final NClob nClob = rows.getNClob(i + 1);
			if (rows.wasNull() || nClob == null) {
				columnData = null;
			} else {
				columnData = readClob(nClob, null);
			}
		} else if (javaSqlType == Types.BLOB) {
			final Blob blob = rows.getBlob(i + 1);
			if (rows.wasNull() || blob == null) {
				columnData = null;
			} else {
				columnData = readBlob(blob);
			}
		} else if (javaSqlType == Types.LONGVARBINARY || javaSqlType == Types.VARBINARY) {

			if (resultsColumns.get(i).getColumnDataType().getName().equalsIgnoreCase("RAW")) {

				try {
					final Blob blob = rows.getBlob(i + 1);
					final InputStream stream = rows.getBinaryStream(i + 1);
					if (rows.wasNull() || stream == null) {
						columnData = null;
					} else {
						columnData = readStream(stream, blob);
					}
				} catch (Exception e) {
					final InputStream stream = rows.getAsciiStream(i + 1);
					if (rows.wasNull() || stream == null) {
						columnData = null;
					} else {
						columnData = readStream(stream, null);
					}
				}

			} else if (resultsColumns.get(i).getColumnDataType().getName().equalsIgnoreCase("VARCHAR FOR BIT DATA")) {
				// final Object objectValue;
				// byte[] bytes = rows.getBytes(i + 1);
				// String s = new String(bytes, StandardCharsets.UTF_8);
				// objectValue = s;
				// if (rows.wasNull() || objectValue == null)
				// columnData = null;
				// else
				// columnData = objectValue;

				final Object objectValue = rows.getString(i + 1);
				if (rows.wasNull() || objectValue == null)
					columnData = null;
				else
					columnData = objectValue;

			} else {

				try {
					final Blob blob = rows.getBlob(i + 1);
					final InputStream stream = rows.getBinaryStream(i + 1);
					if (rows.wasNull() || stream == null) {
						columnData = null;
					} else {
						columnData = readStream(stream, blob);
					}
				} catch (Exception e) {
					final InputStream stream = rows.getBinaryStream(i + 1);
					if (rows.wasNull() || stream == null) {
						columnData = null;
					} else {
						columnData = readStream(stream, null);
					}
				}

			}
		} else if (javaSqlType == Types.LONGNVARCHAR || javaSqlType == Types.LONGVARCHAR) {
			if (((ResultsColumn) this.resultsColumns.get(i)).getColumnDataType().getName().equalsIgnoreCase("LONG")) {
				final Object objectValue = rows.getObject(i + 1);
				if (rows.wasNull() || objectValue == null)
					columnData = null;
				else
					columnData = objectValue;
			} else {

				try {
					final InputStream stream = rows.getAsciiStream(i + 1);
					if (rows.wasNull() || stream == null) {
						columnData = null;
					} else {
						columnData = readStream(stream, null);
					}
				} catch (Exception e) {
					SQLXML xmlVal = rows.getSQLXML(i + 1);
					String xml = xmlVal.getString();
					if (rows.wasNull() || xml == null) {
						columnData = null;
					} else {
						columnData = (xml);
					}
				}

			}
		} else if (javaSqlType == Types.DATE || ((ResultsColumn) this.resultsColumns.get(i)).getColumnDataType()
				.getName().equalsIgnoreCase("DATE")) {
			final Date datevalue = rows.getDate(i + 1);
			if (rows.wasNull() || datevalue == null) {
				columnData = null;
			} else {
				try {
					// System.out.println("DATE VALUE : " + rows.getString(i +
					// 1));
					java.sql.Date ts = rows.getDate(i + 1);
					Date date = new Date();
					date.setTime(ts.getTime());
					String formattedDate;
					if (showDateTime)
						formattedDate = dateFormat.format(date);
					else
						formattedDate = dateOnlyFormat.format(date);
					columnData = formattedDate;
				} catch (Exception e) {
					// System.out.println("Date excpetion");
					columnData = rows.getString(i + 1);
				}
			}
		} else if (javaSqlType == Types.TIMESTAMP || javaSqlType == Types.TIMESTAMP_WITH_TIMEZONE
				|| resultsColumns.get(i).getColumnDataType().getName().equalsIgnoreCase("TIMESTAMP WITH TIME ZONE")) {
			final Timestamp timestampValue = rows.getTimestamp(i + 1);
			if (rows.wasNull() || timestampValue == null) {
				columnData = null;
			} else {
				try {
					// System.out.println("TIMESTAMP VALUE : " +
					// rows.getString(i + 1) + " / " +
					// rows.getTimestamp(i + 1));
					Timestamp ts = rows.getTimestamp(i + 1);
					Date date = new Date();
					date.setTime(ts.getTime());
					String formattedDate = dateFormat.format(date);
					columnData = formattedDate;
				} catch (Exception e) {
					// System.out.println("Timestamp excpetion");
					columnData = rows.getTimestamp(i + 1);
				}
			}
			// } else if (javaSqlType == Types.BIGINT) {
			// final long bigIntValue = rows.getLong(i + 1);
			// if (rows.wasNull()) {
			// columnData = null;
			// } else {
			// columnData = bigIntValue;
			// }
			// } else if (javaSqlType == Types.TINYINT) {
			// final byte byteValue = rows.getByte(i + 1);
			// if (rows.wasNull()) {
			// columnData = null;
			// } else {
			// columnData = byteValue;
			// }
			// } else if (javaSqlType == Types.SMALLINT) {
			// final short shortValue = rows.getShort(i + 1);
			// if (rows.wasNull()) {
			// columnData = null;
			// } else {
			// columnData = shortValue;
			// }
		} else if (javaSqlType == Types.BIT) {
			final boolean booleanValue = rows.getBoolean(i + 1);
			final String stringValue = rows.getString(i + 1);
			if (rows.wasNull()) {
				columnData = null;
			} else {
				columnData = stringValue.equalsIgnoreCase(Boolean.toString(booleanValue)) ? booleanValue : stringValue;
			}
			// } else if (javaSqlType == Types.INTEGER) {
			// final int intValue = rows.getInt(i + 1);
			// if (rows.wasNull()) {
			// columnData = null;
			// } else {
			// columnData = intValue;
			// }
		} else if (javaSqlType == Types.FLOAT /* || javaSqlType == Types.REAL */) {
			final float floatValue = rows.getFloat(i + 1);
			if (rows.wasNull()) {
				columnData = null;
			} else {
				float value = (float) floatValue;
				if (Math.abs(value - (int) value) > 0.0)
					formatter = new DecimalFormat(
							"#.##########################################################################################################################################################################################################################");
				else
					formatter = new DecimalFormat("#");
				columnData = formatter.format(value);
			}
		} else if (javaSqlType == Types.DOUBLE) {
			final double doubleValue = rows.getDouble(i + 1);
			if (rows.wasNull()) {
				columnData = null;
			} else {
				double value = (double) doubleValue;
				if (Math.abs(value - (int) value) > 0.0)
					formatter = new DecimalFormat(
							"#.##########################################################################################################################################################################################################################");
				else
					formatter = new DecimalFormat("#");
				columnData = formatter.format(value);
			}
		} else {
			// System.out.println("STRING " +
			// resultsColumns.get(i).getColumnDataType().getName());

			if (resultsColumns.get(i).getColumnDataType().getName().equalsIgnoreCase("CHAR FOR BIT DATA")) {
				// final Object objectValue;
				// byte[] bytes = rows.getBytes(i + 1);
				// String s = new String(bytes, StandardCharsets.UTF_8);
				// objectValue = s;
				// if (rows.wasNull() || objectValue == null)
				// columnData = null;
				// else
				// columnData = objectValue;

				// final Object objectValue =
				// UUID.nameUUIDFromBytes(rows.getBytes(i + 1));
				// if (rows.wasNull() || objectValue == null)
				// columnData = null;
				// else
				// columnData = objectValue;

				final Object objectValue = rows.getString(i + 1);
				if (rows.wasNull() || objectValue == null)
					columnData = null;
				else
					columnData = objectValue;

			} else {
				final Object objectValue = rows.getObject(i + 1);
				if (rows.wasNull() || objectValue == null)
					columnData = null;
				else
					columnData = objectValue;
			}
		}
		return columnData;
	}

	public BinaryData readBlob(final Blob blob) {
		if (blob == null) {
			return null;
		} else if (showLobs) {
			InputStream in = null;
			BinaryData lobData;
			try {
				try {
					in = blob.getBinaryStream();
				} catch (final SQLFeatureNotSupportedException e) {
					LOGGER.log(Level.FINEST, "Could not read BLOB data", e);
					in = null;
				}

				if (in != null) {
					lobData = new BinaryData(readFully(in), blob);
				} else {
					lobData = new BinaryData();
				}
			} catch (final SQLException e) {
				LOGGER.log(Level.WARNING, "Could not read BLOB data", e);
				lobData = new BinaryData();
			}
			return lobData;
		} else {
			return new BinaryData();
		}
	}

	public BinaryData readClob(final Clob clob, final Blob blob) {
		if (clob == null) {
			return null;
		} else if (showLobs) {
			Reader rdr = null;
			BinaryData lobData;
			try {
				try {
					rdr = clob.getCharacterStream();
				} catch (final SQLFeatureNotSupportedException e) {
					LOGGER.log(Level.FINEST, "Could not read CLOB data, as character stream", e);
					rdr = null;
				}
				if (rdr == null) {
					try {
						rdr = new InputStreamReader(clob.getAsciiStream());
					} catch (final SQLFeatureNotSupportedException e) {
						LOGGER.log(Level.FINEST, "Could not read CLOB data, as ASCII stream", e);
						rdr = null;
					}
				}

				if (rdr != null) {
					String lobDataString = readFully(rdr);
					if (lobDataString.isEmpty()) {
						// Attempt yet another read
						final long clobLength = clob.length();
						lobDataString = clob.getSubString(1, (int) clobLength);
					}
					lobData = new BinaryData(lobDataString, blob);
				} else {
					lobData = new BinaryData();
				}
			} catch (final SQLException e) {
				LOGGER.log(Level.WARNING, "Could not read CLOB data", e);
				lobData = new BinaryData();
			}
			return lobData;
		} else {
			return new BinaryData();
		}
	}

	/**
	 * Reads data from an input stream into a string. Default system encoding is
	 * assumed.
	 *
	 * @param columnData
	 *            Column data object returned by JDBC
	 * @return A string with the contents of the LOB
	 */
	private BinaryData readStream(final InputStream stream, final Blob blob) {
		if (stream == null) {
			return null;
		} else if (showLobs) {
			final BufferedInputStream in = new BufferedInputStream(stream);
			final BinaryData lobData = new BinaryData(readFully(in), blob);
			return lobData;
		} else {
			return new BinaryData();
		}
	}

}
