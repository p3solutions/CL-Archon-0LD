package com.p3.archon.coreprocess.executables;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;

import javax.sql.DataSource;

import com.p3.archon.coreprocess.beans.ArchonInputBean;
import com.p3.archon.coreprocess.common.Constants;

import schemacrawler.schema.Catalog;
import schemacrawler.schema.Column;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;
import schemacrawler.schema.View;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaInfoLevelBuilder;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.tools.text.operation.OperationExecutable;
import schemacrawler.tools.text.operation.OperationOptions;
import schemacrawler.utility.SchemaCrawlerUtility;

public class TestExecutionJob extends ExecutionJob implements JobExecutable {

	private static final String COUNTOPENTAG = "<COUNT>";
	private static final String COUNTCLOSETAG = "</COUNT>";

	public TestExecutionJob(DataSource dataSource, Connection connection, ArchonInputBean inputArgs) {
		super(dataSource, connection, inputArgs);
	}

	@Override
	public void start() throws Exception {
		printLines(new String[] { "Row Count", "---------", "" });
		boolean version = inputArgs.getVersion().trim().equals("4");
		
		final SchemaCrawlerOptions testCountOptions = schemaCrawlerOptionsBuilder(inputArgs, false,
				SchemaInfoLevelBuilder.standard(), true, false, false);
		OperationExecutable opterationExe = new OperationExecutable("count");
		OperationOptions operationOptions = new OperationOptions();
		operationOptions.setShowUnqualifiedNames(true);
		operationOptions.setVersion4(version);
		operationOptions.setXmlChunkLimit(Constants.MAX_RECORD_PER_XML_FILE);
		operationOptions.setShowLobs(Constants.SHOW_LOB);
		operationOptions.setDateWithDateTime(Constants.SHOW_DATETIME);
		operationOptions.setSipExtract(Constants.IS_SIP_EXTRACT);
		operationOptions.setAppendOutput(true);
		opterationExe.setSchemaCrawlerOptions(testCountOptions);

		OutputOptions outputOptions = new OutputOptions();
		outputOptions.setOutputFormatValue(inputArgs.outputFormat);
		Path path = null;
		if (!inputArgs.outputFormat.equalsIgnoreCase("console")) {
			String outputPath = "Output";
			if (inputArgs.outputPath != null && !inputArgs.outputPath.equalsIgnoreCase(""))
				outputPath = inputArgs.outputPath;
			new File(outputPath).mkdirs();
			path = Paths.get(outputPath + File.separator + "RowCount" + "." + inputArgs.outputFormat);
			if (inputArgs.outputFormat.equalsIgnoreCase("xml")) {
				FileWriter writer = new FileWriter(path.toString());
				writer.write(COUNTOPENTAG);
				writer.flush();
				writer.close();
			}
			outputOptions.setOutputFile(path);
		}

		opterationExe.setOutputOptions(outputOptions);
		opterationExe.setOperationOptions(operationOptions);
		final Catalog tableDumpCatalog = SchemaCrawlerUtility.getCatalog(connection, testCountOptions);
		
		
		final Catalog catalog = SchemaCrawlerUtility.getCatalog(connection, testCountOptions);

		for (final Schema schema : catalog.getSchemas()) {
			System.out.println(schema);
			for (final Table table : catalog.getTables(schema)) {
				System.out.print("o--> " + table);
				if (table instanceof View) {
					System.out.println(" (VIEW)");
				} else {
					System.out.println();
				}
				System.out.println("--------------------");
				for (Column col : table.getColumns()) {
					System.out.println(col.getFullName());
				}
				System.out.println("--------------------");
				System.out.println("\n\n\n");
			}
		}
		
		System.out.println("Executing row count query and gathering results");
		opterationExe.executeOn(tableDumpCatalog, connection);
		if (inputArgs.outputFormat.equalsIgnoreCase("xml")) {
			FileWriter writer = new FileWriter(path.toString(), true);
			writer.write("\n");
			writer.write(COUNTCLOSETAG);
			writer.flush();
			writer.close();
		}
	}
}
