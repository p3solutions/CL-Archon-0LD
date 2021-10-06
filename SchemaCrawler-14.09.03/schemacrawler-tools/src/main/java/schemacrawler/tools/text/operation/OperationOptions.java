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

import schemacrawler.tools.text.base.BaseTextOptions;

/**
 * Operator options.
 *
 * @author Sualeh Fatehi
 * @modified by Malik
 */
public final class OperationOptions extends BaseTextOptions {

	private static final long serialVersionUID = -7977434852526746391L;

	private boolean isDateWithDateTime;
	private boolean isSipExtract;
	private boolean isShowLobs;
	private int xmlChunkLimit;
	private String tableNameQuickDump;
	private boolean version4;
	private String schema;

	// DBLink DataPull
	private boolean dbLinkPull;
	private String dbLinkSchema;
	private String dbLinkTableName;
	private String dbLinkFullTableName;

	// Unstructured Data
	private String blobTableName;
	private String blobColumnName;
	private boolean seqFilename = true;
	private int seqStartValue = 1;

	private boolean filenameOverride = false;
	private String filenameOverrideValue = "";

	private boolean filename = false;
	private String filenameColumn = "";

	private String blobFolderidentifier = "X";

	/**
	 * Whether to show LOBs.
	 *
	 * @return Whether to show LOBs.
	 */
	public boolean isShowLobs() {
		return isShowLobs;
	}

	/**
	 * Whether to show LOBs.
	 *
	 * @param showLobs
	 *            Whether to show LOBs
	 */
	public void setShowLobs(final boolean showLobs) {
		isShowLobs = showLobs;
	}

	public boolean isSipExtract() {
		return isSipExtract;
	}

	public void setSipExtract(boolean isSipExtract) {
		this.isSipExtract = isSipExtract;
	}

	public boolean isDateWithDateTime() {
		return isDateWithDateTime;
	}

	public void setDateWithDateTime(boolean isDateWithDateTime) {
		this.isDateWithDateTime = isDateWithDateTime;
	}

	public int getXmlChunkLimit() {
		if (xmlChunkLimit == 0)
			return 500000;
		else
			return xmlChunkLimit;
	}

	public void setXmlChunkLimit(int xmlChunkLimit) {
		this.xmlChunkLimit = xmlChunkLimit;
	}

	public String getTableNameQuickDump() {
		return tableNameQuickDump;
	}

	public void setTableNameQuickDump(String tableNameQuickDump) {
		this.tableNameQuickDump = tableNameQuickDump;
	}

	public boolean isVersion4() {
		return version4;
	}

	public void setVersion4(boolean version4) {
		this.version4 = version4;
	}

	public String getFilenameColumn() {
		return filenameColumn;
	}

	public void setFilenameColumn(String filenameColumn) {
		this.filenameColumn = filenameColumn;
	}

	public boolean isFilename() {
		return filename;
	}

	public void setFilename(boolean filename) {
		this.filename = filename;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public boolean isSeqFilename() {
		return seqFilename;
	}

	public void setSeqFilename(boolean seqFilename) {
		this.seqFilename = seqFilename;
	}

	public int getSeqStartValue() {
		return seqStartValue;
	}

	public void setSeqStartValue(int seqStartValue) {
		this.seqStartValue = seqStartValue;
	}

	public boolean isFilenameOverride() {
		return filenameOverride;
	}

	public void setFilenameOverride(boolean filenameOverride) {
		this.filenameOverride = filenameOverride;
	}

	public String getFilenameOverrideValue() {
		return filenameOverrideValue;
	}

	public void setFilenameOverrideValue(String filenameOverrideValue) {
		this.filenameOverrideValue = filenameOverrideValue;
	}

	public String getBlobColumnName() {
		return blobColumnName;
	}

	public void setBlobColumnName(String blobColumnName) {
		this.blobColumnName = blobColumnName;
	}

	public String getBlobTableName() {
		return blobTableName;
	}

	public void setBlobTableName(String blobTableName) {
		this.blobTableName = blobTableName;
	}

	public boolean isDbLinkPull() {
		return dbLinkPull;
	}

	public void setDbLinkPull(boolean dbLinkPull) {
		this.dbLinkPull = dbLinkPull;
	}

	public String getDbLinkSchema() {
		return dbLinkSchema;
	}

	public void setDbLinkSchema(String dbLinkSchema) {
		this.dbLinkSchema = dbLinkSchema;
	}

	public String getDbLinkTableName() {
		return dbLinkTableName;
	}

	public void setDbLinkTableName(String dbLinkTableName) {
		this.dbLinkTableName = dbLinkTableName;
	}

	public String getDbLinkFullTableName() {
		return dbLinkFullTableName;
	}

	public void setDbLinkFullTableName(String dbLinkFullTableName) {
		this.dbLinkFullTableName = dbLinkFullTableName;
	}

	public String getBlobFolderidentifier() {
		return blobFolderidentifier;
	}

	public void setBlobFolderidentifier(String blobFolderidentifier) {
		this.blobFolderidentifier = blobFolderidentifier;
	}

}
