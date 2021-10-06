package com.p3.archon.coreprocess.executables;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import com.p3.archon.coreprocess.beans.ArchonInputBean;
import com.p3.archon.coreprocess.common.Constants;
import com.p3.archon.coreprocess.validations.Validations;
import com.p3.archon.utils.FileUtil;

import schemacrawler.schema.Catalog;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.IncludeAll;
import schemacrawler.schemacrawler.RegularExpressionInclusionRule;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaInfoLevelBuilder;
import schemacrawler.tools.analysis.counts.CatalogWithCounts;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.tools.text.schema.SchemaTextExecutable;
import schemacrawler.tools.text.schema.SchemaTextOptions;
import schemacrawler.utility.SchemaCrawlerUtility;

public class SchemaExecutionJob extends ExecutionJob implements JobExecutable {

	int maxThread = Constants.MAX_SCHEMA_PARALLEL_EXT;
	int counter = 0;

	public SchemaExecutionJob(DataSource dataSource, Connection connection, ArchonInputBean inputArgs) {
		super(dataSource, connection, inputArgs);
	}

	@Override
	public void start() throws Exception {
		printLines(new String[] { "Schema Generation", "-----------------", "" });
		final SchemaCrawlerOptions schemaOption = schemaCrawlerOptionsBuilder(inputArgs, false,
				SchemaInfoLevelBuilder.maximum(), true, true, true);
		final Catalog catalog = SchemaCrawlerUtility.getCatalog(connection, schemaOption);
		String outputPath = "Output";
		if (inputArgs.outputPath != null && !inputArgs.outputPath.equalsIgnoreCase(""))
			outputPath = inputArgs.outputPath;
		new File(outputPath).mkdirs();
		SchemaTextOptions schemaTextOptions = new SchemaTextOptions();
		schemaTextOptions.setShowOrdinalNumbers(true);
		schemaTextOptions.setShowStandardColumnTypeNames(true);
		schemaTextOptions.setShowUnqualifiedNames(true);
		schemaTextOptions.setNoInfo(true);
		schemaTextOptions.setAlphabeticalSortForForeignKeys(true);
		schemaTextOptions.setAlphabeticalSortForIndexes(true);
		schemaTextOptions.setAlphabeticalSortForRoutineColumns(true);
		schemaTextOptions.setAlphabeticalSortForTables(true);
		if (!inputArgs.outputFormat.equalsIgnoreCase("xml")) {
			OutputOptions outputOptions = new OutputOptions();
			outputOptions.setOutputFormatValue(inputArgs.outputFormat);
			Path path = null;
			if (!inputArgs.outputFormat.equalsIgnoreCase("console")) {
				if (schemaOption.getTableInclusionRule().getInclusionPattern().toString()
						.endsWith(inputArgs.schemaName + ".*")) {
					if (inputArgs.command.equalsIgnoreCase("metadata"))
						path = Paths.get(outputPath + File.separator + "All_Table_metadata." + inputArgs.outputFormat);
					else
						path = Paths.get(outputPath + File.separator + "All_Table_schema." + inputArgs.outputFormat);
				} else if (inputArgs.command.equalsIgnoreCase("metadata"))
					path = Paths
							.get(outputPath + File.separator + "metadata_selective_tables." + inputArgs.outputFormat);
				else
					path = Paths.get(outputPath + File.separator + "selective_tables_schema." + inputArgs.outputFormat);
				outputOptions.setOutputFile(path);
			}
			SchemaTextExecutable schemaTextExe = new SchemaTextExecutable("schema");
			schemaTextExe.setOutputOptions(outputOptions);
			schemaTextExe.setSchemaTextOptions(schemaTextOptions);
			final Catalog schemaCatalog = SchemaCrawlerUtility.getCatalog(connection, schemaOption);
			schemaTextExe.executeOn(schemaCatalog, connection);
			if (Constants.MOVE_FILE_TO_NAS)
				FileUtil.movefile(path.toFile().getAbsolutePath(), Constants.NAS_FILE_PATH, false);
		} else {
			final List<Table> allTables = new ArrayList<>(catalog.getTables());
			for (final Table table : allTables) {
				while (counter == maxThread) {
					Thread.sleep(10);
				}
				Thread t = new Thread(runnerSchema(table, outputPath, schemaTextOptions));
				counter++;
				t.start();
				Thread.sleep(5);
			}
		}

		while (counter != 0) {
			Thread.sleep(2);
		}
	}

	private Runnable runnerSchema(Table table, String outputPath, SchemaTextOptions schemaTextOptions) {
		return new Runnable() {

			@Override
			public void run() {
				System.gc();
				System.out.println("Processing Schema for Table " + table.getName());
				SchemaTextExecutable schemaTextExe = new SchemaTextExecutable("schema");
				final SchemaCrawlerOptions schemaOptions = new SchemaCrawlerOptions();
				schemaOptions.setSchemaInfoLevel(SchemaInfoLevelBuilder.detailed());
				schemaOptions.setRoutineInclusionRule(new IncludeAll());
				schemaOptions.setColumnInclusionRule(new IncludeAll());
				schemaOptions.setRoutineColumnInclusionRule(new IncludeAll());
				schemaOptions.setRoutineInclusionRule(new IncludeAll());

				switch (inputArgs.getDatabaseServer().toLowerCase()) {
				case "sql":
				case "sqlwinauth":
				case "sybase":
					schemaOptions.setSchemaInclusionRule(
							new RegularExpressionInclusionRule(inputArgs.databaseName + "." + inputArgs.schemaName));
					break;
				case "teradata":
				case "oracle":
				case "oracleservice":
				case "mysql":
				case "maria":
				case "mariadb":
				case "db2":
				case "postgresql":
					schemaOptions.setSchemaInclusionRule(new RegularExpressionInclusionRule(inputArgs.schemaName));
					break;
				}

				schemaOptions.setTableInclusionRule(new RegularExpressionInclusionRule(table.getFullName()));
				schemaTextExe.setSchemaCrawlerOptions(schemaOptions);
				OutputOptions outputOptions = new OutputOptions();
				outputOptions.setOutputFormatValue(inputArgs.outputFormat);

				Path path = Paths.get(outputPath + File.separator + Validations.checkValidFile(table.getName())
						+ "_schema." + inputArgs.outputFormat);
				outputOptions.setOutputFile(path);
				schemaTextExe.setOutputOptions(outputOptions);
				schemaTextExe.setSchemaTextOptions(schemaTextOptions);
				try {
					Catalog xcatalog = SchemaCrawlerUtility.getCatalog(connection, schemaOptions);
					final Catalog schemaCatalog = new CatalogWithCounts(xcatalog, connection, schemaOptions);
					schemaTextExe.executeOn(schemaCatalog, connection);
				} catch (Exception e) {
					e.printStackTrace();
				}
				counter--;
				System.gc();
			}
		};
	}
}
