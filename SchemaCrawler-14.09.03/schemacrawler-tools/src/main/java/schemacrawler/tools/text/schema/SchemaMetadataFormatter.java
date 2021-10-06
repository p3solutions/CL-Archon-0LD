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

package schemacrawler.tools.text.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import schemacrawler.schema.BaseForeignKey;
import schemacrawler.schema.Column;
import schemacrawler.schema.ColumnDataType;
import schemacrawler.schema.ColumnReference;
import schemacrawler.schema.ForeignKey;
import schemacrawler.schema.Routine;
import schemacrawler.schema.Sequence;
import schemacrawler.schema.Synonym;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.tools.analysis.counts.CountsUtility;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.tools.text.base.BaseXMLFormatter;
import schemacrawler.tools.traversal.SchemaTraversalHandler;
import schemacrawler.utility.MetaDataUtility;
import schemacrawler.utility.MetaDataUtility.ForeignKeyCardinality;
import schemacrawler.utility.NamedObjectSort;

/**
 * JSON formatting of schema.
 *
 * @author Sualeh Fatehi
 */
@SuppressWarnings("unused")
final class SchemaMetadataFormatter extends BaseXMLFormatter<SchemaTextOptions> implements SchemaTraversalHandler {

	private final boolean isVerbose;
	private final boolean isBrief;

	/**
	 * Text formatting of schema.
	 *
	 * @param schemaTextDetailType
	 *            Types for text formatting of schema
	 * @param options
	 *            Options for text formatting of schema
	 * @param outputOptions
	 *            Options for text formatting of schema
	 * @throws SchemaCrawlerException
	 *             On an exception
	 */
	SchemaMetadataFormatter(final SchemaTextDetailType schemaTextDetailType, final SchemaTextOptions options,
			final OutputOptions outputOptions) throws SchemaCrawlerException {
		super(options, schemaTextDetailType == SchemaTextDetailType.details, outputOptions);
		isVerbose = schemaTextDetailType == SchemaTextDetailType.details;
		isBrief = schemaTextDetailType == SchemaTextDetailType.brief;
	}

	/**
	 * Provides information on the database schema.
	 *
	 * @param sequence
	 *            Sequence metadata.
	 */
	@Override
	public void handle(final Sequence sequence) {
	}

	/**
	 * Provides information on the database schema.
	 *
	 * @param synonym
	 *            Synonym metadata.
	 */
	@Override
	public void handle(final Synonym synonym) {
	}

	@Override
	public void startXML() {
		formattingHelper.writeDocumentStart();
		formattingHelper.writeRootElementStart("metadata");
		// if (options.isFullMetadataFile()) {
		writeElement("caseSensitive", "false");
		writeElement("defaultSchema", "--|||--defaultSchema--|||--");
		writeElement("validatingOnIngest", "false");
		writeElement("locale", "en-US");
		// }
		formattingHelper.writeElementStart("schemaMetadataList");
		formattingHelper.writeElementStart("schemaMetadata");
		writeElement("name", "--|||--name--|||--");
		writeElement("tableCount", "--|||--tableCount--|||--");
		formattingHelper.writeElementStart("tableMetadataList");
	}

	@Override
	public void endXML() {
		formattingHelper.writeElementEnd();
		formattingHelper.writeElementEnd();
		formattingHelper.writeElementEnd();
		formattingHelper.writeRootElementEnd();
		formattingHelper.writeDocumentEnd();
	}

	private String countformatter(String rowCountMessage) {
		if (rowCountMessage == null)
			return "0";
		else if (rowCountMessage.trim().equals("empty"))
			return "0";
		else
			return rowCountMessage.replace("rows", "").replace(",", "").trim();
	}

	int ordinalCounter = 0;

	@Override
	public void handle(final Table table) {
		System.out.println("Processing " + table.getName());
		formattingHelper.writeRootElementStart("tableMetadata");
		writeElement("name", getTextFormatted(table.getName().toUpperCase()));
		writeElement("recordCount", countformatter(CountsUtility.getRowCountMessage(table)));
		formattingHelper.writeElementStart("columnList");
		final List<Column> columns = table.getColumns();
		ordinalCounter = 0;
		for (final Column column : columns) {
			if (outputOptions.isSplitDate())
				handleTableColumnSplit(column);
			else
				handleTableColumn(column);
		}
		formattingHelper.writeElementEnd();
		if (options.isShowRelationship())
			printForeignKeys(table);
		formattingHelper.writeElementEnd();

	}

	private void printForeignKeys(Table table) {
		final Collection<ForeignKey> foreignKeysCollection = table.getForeignKeys();
		if (foreignKeysCollection.isEmpty()) {
			return;
		}

		formattingHelper.writeElementStart("relationshipList");
		final List<ForeignKey> foreignKeys = new ArrayList<>(foreignKeysCollection);
		Collections.sort(foreignKeys, NamedObjectSort.getNamedObjectSort(options.isAlphabeticalSortForForeignKeys()));

		for (final ForeignKey foreignKey : foreignKeys)
			if (foreignKey != null)
				printColumnReferences(true, table, foreignKey);
		formattingHelper.writeElementEnd();

	}

	private void printColumnReferences(final boolean isForeignKey, final Table table,
			final BaseForeignKey<? extends ColumnReference> foreignKey) {
		formattingHelper.writeElementStart("relationship");
		final String name = foreignKey.getName();
		writeElement("name", name);
		final ForeignKeyCardinality fkCardinality = MetaDataUtility.findForeignKeyCardinality(foreignKey);
		writeElement("cardinality ", fkCardinality.toString());
		formattingHelper.writeElementStart("joinList");
		for (final ColumnReference columnReference : foreignKey) {
			formattingHelper.writeElementStart("join");
			final Column pkColumn;
			final Column fkColumn;
			final String pkColumnName;
			final String fkColumnName;
			pkColumn = columnReference.getPrimaryKeyColumn();
			fkColumn = columnReference.getForeignKeyColumn();

			boolean isIncoming = false;
			if (pkColumn.getParent().equals(table))
				isIncoming = true;

			pkColumnName = pkColumn.getName();
			fkColumnName = fkColumn.getName();

			writeElement("isPkTable", Boolean.toString(isIncoming));
			writeElement("pkTable", getTextFormatted(pkColumn.getParent().getName()));
			writeElement("pkColumn", getTextFormatted(pkColumnName));
			writeElement("fkTable", getTextFormatted(fkColumn.getParent().getName()));
			writeElement("fkColumn", getTextFormatted(fkColumnName));

			formattingHelper.writeElementEnd();
		}
		formattingHelper.writeElementEnd();
		formattingHelper.writeElementEnd();
	}

	private void handleTableColumn(final Column column) {
		formattingHelper.writeElementStart("column");
		writeElement("name", getTextFormatted(column.getName().toUpperCase()));
		writeElement("ordinal", Integer.toString(column.getOrdinalPosition()));
		writeElement("type",
				xmlDataTypeSetter(column.getColumnDataType().getJavaSqlType().getJavaSqlTypeName().toUpperCase(),
						column.getSize(), column.getDecimalDigits()));
		writeElement("typeLength", Integer.toString(column.getSize()));
		if (outputOptions.isSetIndex())
			writeElement("index", Boolean.toString(column.isPartOfIndex()));
		else
			writeElement("index", "false");
		formattingHelper.writeElementEnd();
	}

	private void handleTableColumnSplit(final Column column) {
		// System.out.println(column.getParent().getName() + "---" +
		// column.getName() +
		// "----" + column.getType().getName()
		// + "-----" +
		// column.getColumnDataType().getJavaSqlType().getJavaSqlTypeName());
		if (column.getColumnDataType().getJavaSqlType().getJavaSqlTypeName().toUpperCase().contains("DATETIME")
				|| column.getColumnDataType().getJavaSqlType().getJavaSqlTypeName().toUpperCase()
						.contains("TIMESTAMP")) {
			formattingHelper.writeElementStart("column");
			writeElement("name", getTextFormatted(column.getName().toUpperCase()));
			writeElement("ordinal", Integer.toString(++ordinalCounter));
			writeElement("type", "VARCHAR");
			writeElement("typeLength", Integer.toString(column.getSize()));
			if (outputOptions.isSetIndex())
				writeElement("index", Boolean.toString(column.isPartOfIndex()));
			else
				writeElement("index", "false");
			formattingHelper.writeElementEnd();

			formattingHelper.writeElementStart("column");
			writeElement("name", getTextFormatted(column.getName().toUpperCase()) + "_DT_SPLIT");
			writeElement("ordinal", Integer.toString(++ordinalCounter));
			writeElement("type", "DATE");
			writeElement("typeLength", Integer.toString(255));
			if (outputOptions.isSetIndex())
				writeElement("index", Boolean.toString(column.isPartOfIndex()));
			else
				writeElement("index", "false");
			formattingHelper.writeElementEnd();

			formattingHelper.writeElementStart("column");
			writeElement("name", getTextFormatted(column.getName().toUpperCase()) + "_TM_SPLIT");
			writeElement("ordinal", Integer.toString(++ordinalCounter));
			writeElement("type", "TIME");
			writeElement("typeLength", Integer.toString(255));
			if (outputOptions.isSetIndex())
				writeElement("index", Boolean.toString(column.isPartOfIndex()));
			else
				writeElement("index", "false");
			formattingHelper.writeElementEnd();

			formattingHelper.writeElementStart("column");
			writeElement("name", getTextFormatted(column.getName().toUpperCase()) + "_TM_SPLIT_FORMATTED");
			writeElement("ordinal", Integer.toString(++ordinalCounter));
			writeElement("type", "TIME");
			writeElement("typeLength", Integer.toString(255));
			if (outputOptions.isSetIndex())
				writeElement("index", Boolean.toString(column.isPartOfIndex()));
			else
				writeElement("index", "false");
			formattingHelper.writeElementEnd();

			formattingHelper.writeElementStart("column");
			writeElement("name", getTextFormatted(column.getName().toUpperCase()) + "_DTM_SPLIT");
			writeElement("ordinal", Integer.toString(++ordinalCounter));
			writeElement("type", "DATETIME");
			writeElement("typeLength", Integer.toString(255));
			if (outputOptions.isSetIndex())
				writeElement("index", Boolean.toString(column.isPartOfIndex()));
			else
				writeElement("index", "false");
			formattingHelper.writeElementEnd();
		} else {
			formattingHelper.writeElementStart("column");
			writeElement("name", getTextFormatted(column.getName().toUpperCase()));
			writeElement("ordinal", Integer.toString(++ordinalCounter));
			writeElement("type",
					xmlDataTypeSetter(column.getColumnDataType().getJavaSqlType().getJavaSqlTypeName().toUpperCase(),
							column.getSize(), column.getDecimalDigits()));
			writeElement("typeLength", Integer.toString(column.getSize()));
			if (outputOptions.isSetIndex())
				writeElement("index", Boolean.toString(column.isPartOfIndex()));
			else
				writeElement("index", "false");
			formattingHelper.writeElementEnd();
		}
	}

	private String xmlDataTypeSetter(String type, int size, int decimalDigits) {
		type = type.replace("IDENTITY", "").replace("identity", "").replace("Identity", "").trim();
		type = type.replace("UNSIGNED", "").replace("unsigned", "").replace("Unsigned", "").trim();
		type = type.replace("SIGNED", "").replace("signed", "").replace("Signed", "").trim();
		// return type;

		switch (type) {
		case "BOOLEAN":
		case "BIT":
			return "BOOLEAN";
		case "DATE":
			return "DATE";
		case "TIME":
			return "TIME";
		case "DATETIME":
		case "DATETIME2":
		case "TIMESTAMP":
			return "VARCHAR";
		case "BLOB":
		case "CLOB":
		case "TEXT":
		case "NTEXT":
		case "CHAR":
		case "NCHAR":
		case "VARCHAR":
		case "NVARCHAR":
		case "NVARBINARY":
		case "VARBINARY":
		case "VARCHAR2":
		case "RAW":
		case "LONG RAW":
		case "CHARACTER":
		case "CHARACTER VARYING":
		case "BINARY VARYING":
		case "INTERVAL":
		case "TIMESTAMP WITH LOCAL TIME ZONE":
		case "TIMESTAMP WITH TIME ZONE":
		case "SMALLDATETIME":
		case "DATETIMEOFFSET":
			return "VARCHAR";
		case "INT":
		case "INTEGER":
		case "AUTONUMBER":
		case "SMALLINT":
		case "BIGINT":
		case "TINYINT":
			return "INTEGER";
		case "NUMERIC":
			if (decimalDigits > 0)
				return "DOUBLE";
			else if (size > 8)
				return "LONG";
			else
				return "INTEGER";
		case "MONEY":
		case "DEC":
		case "DECIMAL":
		case "SMALLMONEY":
			return "DECIMAL";
		case "DOUBLE":
			return "DOUBLE";
		case "FLOAT":
		case "REAL":
			return "FLOAT";
		case "LONG":
		case "LONGVARCHAR":
			return "LONG";
		default:
			return "VARCHAR";
		}

	}

	private void writeElement(String name, String value) {
		formattingHelper.writeElementStart(name);
		formattingHelper.writeValue(value);
		formattingHelper.writeElementEnd();
	}

	@Override
	public void handle(ColumnDataType columnDataType) throws SchemaCrawlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handle(Routine routine) throws SchemaCrawlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleColumnDataTypesEnd() throws SchemaCrawlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleColumnDataTypesStart() throws SchemaCrawlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleRoutinesEnd() throws SchemaCrawlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleRoutinesStart() throws SchemaCrawlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleSequencesEnd() throws SchemaCrawlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleSequencesStart() throws SchemaCrawlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleSynonymsEnd() throws SchemaCrawlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleSynonymsStart() throws SchemaCrawlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleTablesEnd() throws SchemaCrawlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleTablesStart() throws SchemaCrawlerException {
		// TODO Auto-generated method stub

	}

}
