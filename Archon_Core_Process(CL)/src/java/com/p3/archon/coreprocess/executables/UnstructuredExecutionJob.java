package com.p3.archon.coreprocess.executables;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;

import javax.sql.DataSource;

import com.p3.archon.coreprocess.beans.ArchonInputBean;
import com.p3.archon.coreprocess.beans.FileExtInputBean;
import com.p3.archon.coreprocess.common.Constants;
import com.p3.archon.coreprocess.validations.Validations;

import schemacrawler.schema.Catalog;
import schemacrawler.schemacrawler.Config;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaInfoLevelBuilder;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.tools.text.operation.OperationExecutable;
import schemacrawler.tools.text.operation.OperationOptions;
import schemacrawler.utility.SchemaCrawlerUtility;

public class UnstructuredExecutionJob extends ExecutionJob implements JobExecutable {

	private FileExtInputBean feib;

	public UnstructuredExecutionJob(DataSource dataSource, Connection connection, ArchonInputBean inputArgs)
			throws Exception {
		super(dataSource, connection, inputArgs);
		feib = inputArgs.checkUnstructuredInputs();
	}

	@Override
	public void start() throws Exception {

		printLines(new String[] { "Unstructred Data Extraction", "---------------------------", "" });

		boolean fileNameExtOption = feib.isFileNameExtOption();
		String fileNameOption = feib.getFileNameOption();
		String fileNamePrefixSeq = feib.getFileNamePrefixSeq();
		String fileNameColumn = feib.getFileNameColumn();
		String fileNameExtColumn = feib.getFileNameExtColumn();
		String fileNameExtValue = feib.getFileNameExtValue();
		String blobColumn = feib.getBlobColumn();
		String tableName = feib.getTableName();
		int fileNameStartSeq = feib.getFileNameStartSeq();

		final SchemaCrawlerOptions udeOptions = schemaCrawlerOptionsBuilder(inputArgs, false,
				SchemaInfoLevelBuilder.standard(), true, false, false);

		OperationOptions operationOptions = new OperationOptions();

		boolean version = inputArgs.getVersion().trim().equals("4");
		operationOptions.setShowUnqualifiedNames(true);
		operationOptions.setVersion4(version);
		operationOptions.setShowLobs(true);

		if (inputArgs.getBlobIdentifier() != null)
			operationOptions.setBlobFolderidentifier(inputArgs.getBlobIdentifier());
		switch (fileNameOption.toUpperCase()) {
		case "SEQNAME":
			operationOptions.setFilenameOverride(true);
			operationOptions.setFilenameOverrideValue(fileNamePrefixSeq);
			operationOptions.setSeqStartValue(fileNameStartSeq);
			break;
		case "COLUMN":
			operationOptions.setFilename(true);
			operationOptions.setFilenameColumn(fileNameColumn);
			operationOptions.setSeqStartValue(1);
			break;
		default:
			operationOptions.setSeqFilename(true);
			operationOptions.setSeqStartValue(fileNameStartSeq);
			break;
		}

		OperationExecutable opterationExe = new OperationExecutable("file");
		Config c = new Config();
		StringBuffer sb = new StringBuffer();
		sb.append("select ");
		if (fileNameOption.toUpperCase().equalsIgnoreCase("COLUMN"))
			sb.append(operationOptions.getFilenameColumn()).append(" ,");
		if (fileNameExtOption)
			sb.append(fileNameExtColumn).append(" ,");
		else
			sb.append("'").append(fileNameExtValue).append("' ,");
		sb.append(blobColumn);
		sb.append(" from ").append(getTableFullName(tableName));
		c.put("file", sb.toString());
		opterationExe.setAdditionalConfiguration(c);

		opterationExe.setSchemaCrawlerOptions(udeOptions);
		OutputOptions outputOptions = new OutputOptions();
		outputOptions.setOutputFormatValue(inputArgs.outputFormat);

		operationOptions.setShowUnqualifiedNames(true);
		operationOptions.setShowLobs(true);
		operationOptions.setXmlChunkLimit(Constants.MAX_RECORD_PER_XML_FILE);

		String outputPath = "Output";
		if (inputArgs.outputPath != null && !inputArgs.outputPath.equalsIgnoreCase(""))
			outputPath = inputArgs.outputPath;

		new File(outputPath).mkdirs();
		new File(outputPath + File.separator + "tables").mkdirs();
		new File(outputPath + File.separator + "BLOBs").mkdirs();
		new File(outputPath + File.separator + "BLOBs" + File.separator
				+ Validations.checkValidFolder(tableName.toUpperCase())).mkdirs();
		new File(outputPath + File.separator + "BLOBs" + File.separator
				+ Validations.checkValidFolder(tableName.toUpperCase()) + File.separator
				+ Validations.checkValidFolder(blobColumn.toUpperCase())).mkdirs();
		Path path = Paths.get(outputPath + File.separator + "TEMP.xml");
		outputOptions.setOutputFile(path);
		operationOptions.setAppendOutput(false);
		operationOptions.setSchema(Validations.getTextFormatted(inputArgs.getSchema().toUpperCase()));
		operationOptions.setBlobTableName(Validations.getTextFormatted(tableName.toUpperCase()));
		operationOptions.setBlobColumnName(Validations.getTextFormatted(blobColumn.toUpperCase()));
		opterationExe.setOutputOptions(outputOptions);
		opterationExe.setOperationOptions(operationOptions);

		final Catalog udeCatalog = SchemaCrawlerUtility.getCatalog(connection, udeOptions);
		System.out.println("Executing unstructed Date Extraction Job");
		opterationExe.executeOn(udeCatalog, connection);
		path.toFile().delete();
	}

	private String getTableFullName(String tableName) {
		switch (inputArgs.getDatabaseServer().toLowerCase()) {
		case "sql":
		case "sqlwinauth":
		case "sybase":
			return inputArgs.databaseName + "." + inputArgs.schemaName + "." + tableName;
		case "teradata":
		case "oracle":
		case "oracleservice":
		case "mysql":
		case "maria":
		case "mariadb":
		case "db2":
		case "postgresql":
			return inputArgs.schemaName + "." + tableName;
		default:
			return tableName;
		}
	}

}
