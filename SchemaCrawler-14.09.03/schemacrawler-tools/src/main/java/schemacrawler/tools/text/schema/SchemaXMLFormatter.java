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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import schemacrawler.schema.BaseForeignKey;
import schemacrawler.schema.Column;
import schemacrawler.schema.ColumnDataType;
import schemacrawler.schema.ColumnReference;
import schemacrawler.schema.ConditionTimingType;
import schemacrawler.schema.EventManipulationType;
import schemacrawler.schema.ForeignKey;
import schemacrawler.schema.ForeignKeyColumnReference;
import schemacrawler.schema.ForeignKeyUpdateRule;
import schemacrawler.schema.Grant;
import schemacrawler.schema.Index;
import schemacrawler.schema.IndexColumn;
import schemacrawler.schema.IndexType;
import schemacrawler.schema.PrimaryKey;
import schemacrawler.schema.Privilege;
import schemacrawler.schema.Routine;
import schemacrawler.schema.RoutineColumn;
import schemacrawler.schema.Sequence;
import schemacrawler.schema.Synonym;
import schemacrawler.schema.Table;
import schemacrawler.schema.TableConstraint;
import schemacrawler.schema.TableConstraintColumn;
import schemacrawler.schema.TableConstraintType;
import schemacrawler.schema.Trigger;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.tools.analysis.associations.WeakAssociationForeignKey;
import schemacrawler.tools.analysis.associations.WeakAssociationsUtility;
import schemacrawler.tools.analysis.counts.CountsUtility;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.tools.text.base.BaseXMLFormatter;
import schemacrawler.tools.traversal.SchemaTraversalHandler;
import schemacrawler.utility.NamedObjectSort;

/**
 * JSON formatting of schema.
 *
 * @author Sualeh Fatehi
 */
final class SchemaXMLFormatter extends BaseXMLFormatter<SchemaTextOptions> implements SchemaTraversalHandler {

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
	SchemaXMLFormatter(final SchemaTextDetailType schemaTextDetailType, final SchemaTextOptions options,
			final OutputOptions outputOptions) throws SchemaCrawlerException {
		super(options, schemaTextDetailType == SchemaTextDetailType.details, outputOptions);
		isVerbose = schemaTextDetailType == SchemaTextDetailType.details;
		isBrief = schemaTextDetailType == SchemaTextDetailType.brief;
	}

	@Override
	public void handle(final ColumnDataType columnDataType) throws SchemaCrawlerException {
		if (printVerboseDatabaseInfo && isVerbose) {
			final String databaseSpecificTypeName;
			if (options.isShowUnqualifiedNames()) {
				databaseSpecificTypeName = columnDataType.getName();
			} else {
				databaseSpecificTypeName = columnDataType.getFullName();
			}
			writeElement("DATABASE_SPECIFIC_TYPE_NAME", databaseSpecificTypeName);
			writeElement("BASED_ON",
					columnDataType.getBaseType() == null ? "" : columnDataType.getBaseType().getName());
			writeElement("USER_DEFINED", convertBooleanToString(columnDataType.isUserDefined()));
			writeElement("createParameters", columnDataType.getCreateParameters());
			writeElement("nullable", convertBooleanToString(columnDataType.isNullable()));
			writeElement("autoIncrementable", convertBooleanToString(columnDataType.isAutoIncrementable()));
			writeElement("searchable", columnDataType.getSearchable().toString());
		}
	}

	/**
	 * Provides information on the database schema.
	 *
	 * @param routine
	 *            Routine metadata.
	 */
	@Override
	public void handle(final Routine routine) {
		formattingHelper.writeElementStart("ROUTINE-ROW");
		writeElement("NAME", routine.getName());
		if (!options.isShowUnqualifiedNames()) {
			writeElement("FULL_NAME", routine.getFullName());
		}
		writeElement("TYPE", (routine.getRoutineType() == null ? "" : routine.getRoutineType().toString()));
		writeElement("RETURN_TYPE", (routine.getReturnType() == null ? "" : routine.getReturnType().toString()));

		if (!isBrief) {

			formattingHelper.writeElementStart("ROUTINE_COLUMNS");
			final List<? extends RoutineColumn<? extends Routine>> columns = routine.getColumns();
			Collections.sort(columns,
					NamedObjectSort.getNamedObjectSort(options.isAlphabeticalSortForRoutineColumns()));

			for (final RoutineColumn<?> column : columns) {
				formattingHelper.writeElementStart("ROUTINE_COLUMN");
				handleRoutineColumn(column);
				formattingHelper.writeElementEnd();
			}
			formattingHelper.writeElementEnd();
			if (isVerbose) {
				if (!options.isHideRoutineSpecificNames()) {
					writeElement("SPECIFIC_NAME", routine.getSpecificName());
				}
			}
		}
		formattingHelper.writeElementEnd();
	}

	/**
	 * Provides information on the database schema.
	 *
	 * @param sequence
	 *            Sequence metadata.
	 */
	@Override
	public void handle(final Sequence sequence) {
		formattingHelper.writeElementStart("SEQUENCE-ROW");

		writeElement("NAME", sequence.getName());
		if (!options.isShowUnqualifiedNames()) {
			writeElement("FULL_NAME", sequence.getFullName());
		}
		if (!isBrief) {
			writeElement("INCREMENT", convertLongToString(sequence.getIncrement()));
			writeElement("MINIMUM_VALUE", convertBigIntToString(sequence.getMinimumValue()));
			writeElement("MAXIMUM_VALUE", convertBigIntToString(sequence.getMaximumValue()));
			writeElement("CYCYLE", convertBooleanToString(sequence.isCycle()));
		}
		formattingHelper.writeElementEnd();
	}

	private String convertLongToString(long increment) {
		try {
			return Long.toString(increment);
		} catch (Exception e) {
			return "";
		}
	}

	private String convertBigIntToString(BigInteger maximumValue) {
		if (maximumValue == null)
			return "";
		else
			return maximumValue.toString();
	}

	/**
	 * Provides information on the database schema.
	 *
	 * @param synonym
	 *            Synonym metadata.
	 */
	@Override
	public void handle(final Synonym synonym) {
		formattingHelper.writeElementStart("SYNONYM-ROW");

		writeElement("NAME", synonym.getName());
		if (!options.isShowUnqualifiedNames()) {
			writeElement("FULL_NAME", synonym.getFullName());
		}

		if (!isBrief) {
			final String referencedObjectName;
			if (options.isShowUnqualifiedNames()) {
				referencedObjectName = synonym.getReferencedObject().getName();
			} else {
				referencedObjectName = synonym.getReferencedObject().getFullName();
			}
			writeElement("referencedObject", referencedObjectName);
		}

		formattingHelper.writeElementEnd();
	}

	@Override
	public void startXML() {
		formattingHelper.writeDocumentStart();
		formattingHelper.writeRootElementStart("TABLEMETADATA");
	}

	@Override
	public void endXML() {
		formattingHelper.writeRootElementEnd();
		formattingHelper.writeDocumentEnd();
	}

	@Override
	public void handle(final Table table) {
		System.out.println("Processing " + table.getName());
		formattingHelper.writeAttribute("TABLENAME", getTextFormatted(table.getName().toUpperCase()));

		writeElement("RECORDCOUNT", Long.toString(CountsUtility.getRowCount(table)));
		writeElement("TABLENAME", getTextFormatted(table.getName().toUpperCase()));
		writeElement("TABLEFULLNAME", getTextFormatted(table.getFullName()));
		writeElement("ROOT-ELEMENT", getTextFormatted(table.getName().toUpperCase()));
		writeElement("TYPE", table.getTableType().toString());
		writeElement("ROW-ELEMENT", getTextFormatted(table.getName().toUpperCase()) + "-ROW");

		formattingHelper.writeElementStart("DATATYPES");
		final List<Column> columns = table.getColumns();
		for (final Column column : columns) {
			if (column.isHidden()) {
				continue;
			}
			if (isBrief && !isColumnSignificant(column)) {
				continue;
			}
			formattingHelper.writeElementStart(getTextFormatted(table.getName()) + "-TYPE");
			handleTableColumn(column);
			formattingHelper.writeElementEnd();
		}
		formattingHelper.writeElementEnd();

		formattingHelper.writeElementStart("KEYS");
		formattingHelper.writeElementStart("PRIMARY-KEYS");
		handleIndex(table.getPrimaryKey());
		formattingHelper.writeElementEnd();

		formattingHelper.writeElementStart("FOREIGN-KEYS");
		handleForeignKeys(table.getForeignKeys());
		formattingHelper.writeElementEnd();
		formattingHelper.writeElementEnd();

		if (!isBrief) {
			if (isVerbose) {
				final Collection<WeakAssociationForeignKey> weakAssociationsCollection = WeakAssociationsUtility
						.getWeakAssociations(table);
				final List<WeakAssociationForeignKey> weakAssociations = new ArrayList<>(weakAssociationsCollection);
				Collections.sort(weakAssociations);

				if (!options.isHideWeakAssociations()) {
					formattingHelper.writeElementStart("WEAK_ASSOCIATIONS");
					handleWeakAssociations(weakAssociations);
					formattingHelper.writeElementEnd();
				}
			}
			final Collection<Index> indexesCollection = table.getIndexes();
			final List<Index> indexes = new ArrayList<>(indexesCollection);
			Collections.sort(indexes, NamedObjectSort.getNamedObjectSort(options.isAlphabeticalSortForIndexes()));
			formattingHelper.writeElementStart("INDEXES");
			for (final Index index : indexes) {
				formattingHelper.writeElementStart("INDEX");
				handleIndex(index);
				formattingHelper.writeElementEnd();
			}
			formattingHelper.writeElementEnd();

			formattingHelper.writeElementStart("TRIGGERS");
			handleTriggers(table.getTriggers());
			formattingHelper.writeElementEnd();

			final Collection<TableConstraint> constraintsCollection = table.getTableConstraints();
			final List<TableConstraint> constraints = new ArrayList<>(constraintsCollection);
			Collections.sort(constraints, NamedObjectSort.getNamedObjectSort(options.isAlphabeticalSortForIndexes()));
			formattingHelper.writeElementStart("CONSTRAINTS");
			for (final TableConstraint constraint : constraints) {
				formattingHelper.writeElementStart("CONSTRAINT-ROW");
				handleTableConstraint(constraint);
				formattingHelper.writeElementEnd();
			}
			formattingHelper.writeElementEnd();

			if (isVerbose) {
				formattingHelper.writeElementStart("PRIVILEGES");
				for (final Privilege<Table> privilege : table.getPrivileges()) {
					if (privilege != null) {
						formattingHelper.writeElementStart("PRIVILEGE-ROW");
						writeElement("NAME", privilege.getName());
						for (final Grant<?> grant : privilege.getGrants()) {
							formattingHelper.writeElementStart("PRIVILEGE-DETAILS");
							writeElement("GRANTOR", grant.getGrantor());
							writeElement("GRANTEE", grant.getGrantee());
							writeElement("GRANTABLE", convertBooleanToString(grant.isGrantable()));
							formattingHelper.writeElementEnd();
						}
						formattingHelper.writeElementEnd();
					}
				}
				formattingHelper.writeElementEnd();
			}
		}

	}

	@Override
	public void handleColumnDataTypesEnd() {
		formattingHelper.writeElementEnd();
	}

	@Override
	public void handleColumnDataTypesStart() {
		formattingHelper.writeElementStart("COLUMN_DATA_TYPES");
	}

	@Override
	public void handleRoutinesEnd() throws SchemaCrawlerException {
		formattingHelper.writeElementEnd();
	}

	@Override
	public void handleRoutinesStart() throws SchemaCrawlerException {
		formattingHelper.writeElementStart("ROUTINE");
	}

	@Override
	public void handleSequencesEnd() throws SchemaCrawlerException {
		formattingHelper.writeElementEnd();
	}

	@Override
	public void handleSequencesStart() throws SchemaCrawlerException {
		formattingHelper.writeElementStart("SEQUENCE");
	}

	@Override
	public void handleSynonymsEnd() throws SchemaCrawlerException {
		formattingHelper.writeElementEnd();
	}

	@Override
	public void handleSynonymsStart() throws SchemaCrawlerException {
		formattingHelper.writeElementStart("SYNONYMS");
	}

	@Override
	public void handleTablesEnd() throws SchemaCrawlerException {
	}

	@Override
	public void handleTablesStart() throws SchemaCrawlerException {
	}

	private void handleColumnReferences(final BaseForeignKey<? extends ColumnReference> foreignKey) {
		for (final ColumnReference columnReference : foreignKey) {
			formattingHelper.writeElementStart("COLUMN-REFERENCE");
			final String pkColumnName;
			if (options.isShowUnqualifiedNames()) {
				pkColumnName = columnReference.getPrimaryKeyColumn().getShortName();
			} else {
				pkColumnName = columnReference.getPrimaryKeyColumn().getFullName();
			}
			writeElement("pkColumn", getTextFormatted(pkColumnName));

			final String fkColumnName;
			if (options.isShowUnqualifiedNames()) {
				fkColumnName = columnReference.getForeignKeyColumn().getShortName();
			} else {
				fkColumnName = columnReference.getForeignKeyColumn().getFullName();
			}
			writeElement("fkColumn", getTextFormatted(fkColumnName));

			if (columnReference instanceof ForeignKeyColumnReference && options.isShowOrdinalNumbers()) {
				final int keySequence = ((ForeignKeyColumnReference) columnReference).getKeySequence();
				writeElement("keySequence", convertIntToString(keySequence));
			}

			formattingHelper.writeElementEnd();
		}
	}

	private void handleForeignKeys(final Collection<ForeignKey> foreignKeysCollection) {
		final List<ForeignKey> foreignKeys = new ArrayList<>(foreignKeysCollection);
		Collections.sort(foreignKeys, NamedObjectSort.getNamedObjectSort(options.isAlphabeticalSortForForeignKeys()));
		for (final ForeignKey foreignKey : foreignKeys) {
			if (foreignKey != null) {
				formattingHelper.writeElementStart("FOREIGN-KEY");
				if (!options.isHideForeignKeyNames()) {
					writeElement("NAME", getTextFormatted(foreignKey.getName()));
				}

				final ForeignKeyUpdateRule updateRule = foreignKey.getUpdateRule();
				if (updateRule != null && updateRule != ForeignKeyUpdateRule.unknown) {
					writeElement("UPDATE_RULE", updateRule.toString());
				}

				final ForeignKeyUpdateRule deleteRule = foreignKey.getDeleteRule();
				if (deleteRule != null && deleteRule != ForeignKeyUpdateRule.unknown) {
					writeElement("DELETE_RULE", deleteRule.toString());
				}
				formattingHelper.writeElementStart("COLUMN-REFERENCES");
				handleColumnReferences(foreignKey);
				formattingHelper.writeElementEnd();
				formattingHelper.writeElementEnd();
			}
		}

	}

	private void handleIndex(final Index index) {
		if (index == null || index.getName() == null)
			return;

		if (index instanceof PrimaryKey)
			formattingHelper.writeElementStart("PRIMARY-KEY");
		else
			formattingHelper.writeElementStart("INDEX-ROW");

		if (index instanceof PrimaryKey && !options.isHidePrimaryKeyNames()) {
			writeElement("NAME", getTextFormatted(index.getName()));
		} else if (!options.isHideIndexNames()) {
			writeElement("NAME", getTextFormatted(index.getName()));
		}

		final IndexType indexType = index.getIndexType();
		if (indexType != IndexType.unknown && indexType != IndexType.other) {
			writeElement("TYPE", indexType.toString());
		}
		writeElement("unique", convertBooleanToString(index.isUnique()));

		if (index instanceof PrimaryKey)
			formattingHelper.writeElementStart("PRIMARY-KEY-COLUMNS");
		else
			formattingHelper.writeElementStart("INDEX-ROW-COLUMNS");
		for (final IndexColumn indexColumn : index) {
			if (index instanceof PrimaryKey)
				formattingHelper.writeElementStart("PRIMARY-KEY-COLUMN");
			else
				formattingHelper.writeElementStart("INDEX-ROW-COLUMN");
			handleTableColumn(indexColumn);
			formattingHelper.writeElementEnd();
		}
		formattingHelper.writeElementEnd();

		formattingHelper.writeElementEnd();

	}

	private void handleRoutineColumn(final RoutineColumn<?> column) {

		writeElement("NAME", column.getName());
		writeElement("DATATYPE", column.getColumnDataType().getJavaSqlType().getJavaSqlTypeName());
		writeElement("DATABASE_SPECIFIC_TYPE", column.getColumnDataType().getDatabaseSpecificTypeName());
		writeElement("WIDTH", column.getWidth());
		writeElement("TYPE", column.getColumnType().toString());
		if (options.isShowOrdinalNumbers()) {
			writeElement("COLNO", convertIntToString(column.getOrdinalPosition() + 1));
		}

	}

	private String convertBooleanToString(boolean value) {
		try {
			return Boolean.toString(value);
		} catch (Exception e) {
			return "";
		}
	}

	private String convertIntToString(int value) {
		try {
			return Integer.toString(value);
		} catch (Exception e) {
			return "";
		}
	}

	private void handleTableColumn(final Column column) {
		writeElement("NAME", getTextFormatted(column.getName()));
		if (column instanceof IndexColumn) {
			writeElement("sortSequence", ((IndexColumn) column).getSortSequence().name());
		} else {
			writeElement("COLNO", convertIntToString(column.getOrdinalPosition()));
			writeElement("DATATYPE", column.getColumnDataType().getJavaSqlType().getJavaSqlTypeName());
			writeElement("DATABASE_SPECIFIC_TYPE", column.getColumnDataType().getDatabaseSpecificTypeName());
			writeElement("WIDTH", column.getWidth());
			writeElement("SIZE", convertIntToString(column.getSize()));
			writeElement("DECIMAL_DIGITS", convertIntToString(column.getDecimalDigits()));
			writeElement("IS_NULLABLE", convertBooleanToString(column.isNullable()));
			writeElement("IS_AUTOINCREMENTED", convertBooleanToString(column.isAutoIncremented()));
			writeElement("IS_PART_OF_PK", convertBooleanToString(column.isPartOfPrimaryKey()));
			writeElement("IS_PART_OF_FK", convertBooleanToString(column.isPartOfForeignKey()));
			writeElement("IS_PART_OF_INDEX", convertBooleanToString(column.isPartOfIndex()));
			writeElement("IS_PART_OF_UNIQUE_INDEX", convertBooleanToString(column.isPartOfUniqueIndex()));
			writeElement("DEFAULT_VALUE", (column.getDefaultValue() == null ? "" : column.getDefaultValue()));
			if (options.isShowOrdinalNumbers()) {
				writeElement("ordinal", convertIntToString(column.getOrdinalPosition()));
			}
		}
	}

	private void writeElement(String name, String value) {
		formattingHelper.writeElementStart(name);
		formattingHelper.writeValue(value);
		formattingHelper.writeElementEnd();
	}

	private void handleTableConstraint(final TableConstraint constraint) {
		if (constraint == null)
			return;
		if (!options.isHideTableConstraintNames()) {
			writeElement("NAME", constraint.getName());
		}

		final TableConstraintType tableConstraintType = constraint.getConstraintType();
		if (tableConstraintType != TableConstraintType.unknown) {
			writeElement("TYPE", tableConstraintType.toString());
		}

		for (final TableConstraintColumn tableConstraintColumn : constraint.getColumns()) {
			formattingHelper.writeElementStart("CONSTRAINTS-COLUMNS");
			handleTableColumn(tableConstraintColumn);
			formattingHelper.writeElementEnd();
		}

	}

	private void handleTriggers(final Collection<Trigger> triggers) {
		for (final Trigger trigger : triggers) {
			if (trigger != null) {
				formattingHelper.writeElementStart("TRIGGER");
				if (!options.isHideTriggerNames()) {
					writeElement("NAME", trigger.getName());
				}

				final ConditionTimingType conditionTiming = trigger.getConditionTiming();
				final EventManipulationType eventManipulationType = trigger.getEventManipulationType();
				if (conditionTiming != null && conditionTiming != ConditionTimingType.unknown
						&& eventManipulationType != null && eventManipulationType != EventManipulationType.unknown) {
					writeElement("CONDITION_TIMING", (conditionTiming == null ? "" : conditionTiming.toString()));
					writeElement("EVENT_MANIPULATION_TYPE",
							(eventManipulationType == null ? "" : eventManipulationType.toString()));
				}
				writeElement("ACTION_ORIENTATION",
						(trigger.getActionOrientation() == null ? "" : trigger.getActionOrientation().toString()));
				writeElement("ACTION_CONDITION",
						(trigger.getActionCondition() == null ? "" : trigger.getActionCondition()));
				writeElement("ACTION_STATEMENT",
						(trigger.getActionStatement() == null ? "" : trigger.getActionStatement()));
				formattingHelper.writeElementEnd();
			}
		}
	}

	private void handleWeakAssociations(final Collection<WeakAssociationForeignKey> weakAssociationsCollection) {
		final List<WeakAssociationForeignKey> weakAssociations = new ArrayList<>(weakAssociationsCollection);
		Collections.sort(weakAssociations);
		for (final WeakAssociationForeignKey weakFk : weakAssociations) {
			if (weakFk != null) {
				formattingHelper.writeElementStart("WEAK_ASSOCIATION");
				handleColumnReferences(weakFk);
				formattingHelper.writeElementEnd();
			}
		}
	}
}
