package com.p3.archon.coreprocess.executables;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;

import javax.sql.DataSource;

import com.p3.archon.coreprocess.beans.ArchonInputBean;
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

public class DbLinkQueryExecutionJob extends ExecutionJob implements JobExecutable {

	public DbLinkQueryExecutionJob(DataSource dataSource, Connection connection, ArchonInputBean inputArgs) {
		super(dataSource, connection, inputArgs);
	}

	int maxThread = Constants.MAX_TABLES_PARALLEL_EXT;
	int counter = 0;

	@Override
	public void start() throws Exception {
		printLines(new String[] { "DB Link - DataPull", "---------------", "" });
		boolean version = inputArgs.getVersion().trim().equals("4");
		final SchemaCrawlerOptions queryDumpOptions = schemaCrawlerOptionsBuilder(inputArgs, false,
				SchemaInfoLevelBuilder.standard(), false, false, false);

		FileReader r = new FileReader(inputArgs.queryFile);
		BufferedReader bufferedReader = new BufferedReader(r);
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			String[] info = new String[3];
			int i = 0;
			while (i < 3) {
				try {
					info[i] = line.split(",")[i].trim();
				} catch (ArrayIndexOutOfBoundsException e) {
					info[i] = "";
				}
				i++;
			}
			while (counter == maxThread) {
				Thread.sleep(200);
			}

			Thread t = new Thread(new Runnable() {
				public void run() {
					try {
						// "Select * from " + info[0] + "." + info[1] + "@" + info[2]
						doJob(info, queryDumpOptions, version, inputArgs.schemaName);
						counter--;
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			counter++;
			t.start();
			Thread.sleep(500);
		}
		bufferedReader.close();

		while (counter != 0) {
			Thread.sleep(200);
		}
	}

	private void doJob(String[] info, SchemaCrawlerOptions queryDumpOptions, boolean version, String schema)
			throws Exception {

		String inputArgsqueryTable = info[1]; // get title
		String inputArgsqueryFileName = info[0] + "." + info[1]; // get title
		String inputArgsquery = "Select * from " + info[0] + "." + info[1] + info[2];

		OperationExecutable opterationExe = new OperationExecutable(inputArgsqueryTable);
		OperationOptions operationOptions = new OperationOptions();
		Config c = new Config();
		c.put(inputArgsqueryTable, inputArgsquery);
		opterationExe.setAdditionalConfiguration(c);
		operationOptions.setShowUnqualifiedNames(true);
		operationOptions.setVersion4(version);
		operationOptions.setSchema(schema);
		operationOptions.setXmlChunkLimit(Constants.MAX_RECORD_PER_XML_FILE);
		operationOptions.setShowLobs(Constants.SHOW_LOB);
		operationOptions.setDateWithDateTime(Constants.SHOW_DATETIME);
		operationOptions.setSipExtract(Constants.IS_SIP_EXTRACT);
		opterationExe.setSchemaCrawlerOptions(queryDumpOptions);

		OutputOptions outputOptions = new OutputOptions();
		outputOptions.setOutputFormatValue(inputArgs.outputFormat);
		Path path = null;
		if (!inputArgs.outputFormat.equalsIgnoreCase("console")) {
			String outputPath = "Output";
			if (inputArgs.outputPath != null && !inputArgs.outputPath.equalsIgnoreCase(""))
				outputPath = inputArgs.outputPath;
			new File(outputPath).mkdirs();

			path = Paths.get(outputPath + File.separator
					+ (version ? Validations.checkValidFile(info[0].toUpperCase()) + "-" : "")
					+ Validations.checkValidFile(inputArgsqueryFileName.toUpperCase()) + "."
					+ inputArgs.outputFormat.toLowerCase());
			outputOptions.setOutputFile(path);

		}

		operationOptions.setDbLinkPull(true);
		operationOptions.setDbLinkFullTableName((info[0] + "." + info[1] + info[2]).toUpperCase().trim());
		operationOptions.setDbLinkTableName(info[1].toUpperCase().trim());
		operationOptions.setDbLinkSchema(info[0].toUpperCase().trim());

		opterationExe.setOutputOptions(outputOptions);
		opterationExe.setOperationOptions(operationOptions);
		final Catalog tableDumpCatalog = SchemaCrawlerUtility.getCatalog(connection, queryDumpOptions);
		opterationExe.executeOn(tableDumpCatalog, connection);
		if (inputArgs.outputFormat.equalsIgnoreCase("xml"))
			path.toFile().delete();
	}
}
