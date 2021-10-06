package com.p3.archon.coreprocess.executables;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;

import javax.sql.DataSource;

import com.p3.archon.coreprocess.beans.ArchonInputBean;
import com.p3.archon.coreprocess.common.Constants;
import com.p3.archon.coreprocess.validations.Validations;
import com.p3.archon.utils.FileUtil;

import schemacrawler.schema.Catalog;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaInfoLevelBuilder;
import schemacrawler.tools.analysis.counts.CatalogWithCounts;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.tools.text.schema.SchemaTextExecutable;
import schemacrawler.tools.text.schema.SchemaTextOptions;
import schemacrawler.utility.SchemaCrawlerUtility;

public class MetadataExecutionJob extends ExecutionJob implements JobExecutable {

	private static final String DEFAULTSCHEMA = "--|||--defaultSchema--|||--";
	private static final String NAME = "--|||--name--|||--";
	private static final String TABLECOUNT = "--|||--tableCount--|||--";

	public MetadataExecutionJob(DataSource dataSource, Connection connection, ArchonInputBean inputArgs) {
		super(dataSource, connection, inputArgs);
	}

	@Override
	public void start() throws Exception {
		printLines(new String[] { "Metadata Generation", "-------------------", "" });
		final SchemaCrawlerOptions tableOptions = schemaCrawlerOptionsBuilder(inputArgs, false,
				SchemaInfoLevelBuilder.maximum(), true, false, false);

		SchemaTextExecutable schemaTextExecutable = new SchemaTextExecutable("metadata");
		SchemaTextOptions schemaTextOptions = new SchemaTextOptions();
		schemaTextOptions.setShowOrdinalNumbers(true);
		schemaTextOptions.setShowStandardColumnTypeNames(true);
		schemaTextOptions.setShowUnqualifiedNames(true);
		schemaTextOptions.setNoInfo(true);
		schemaTextOptions.setShowRelationship(Boolean.parseBoolean(inputArgs.includeRelationship));
		schemaTextOptions.setAlphabeticalSortForForeignKeys(true);
		schemaTextOptions.setAlphabeticalSortForIndexes(true);
		schemaTextOptions.setAlphabeticalSortForRoutineColumns(true);
		schemaTextOptions.setAlphabeticalSortForTables(true);

		String outputPath = "Output";
		if (inputArgs.outputPath != null && !inputArgs.outputPath.equalsIgnoreCase(""))
			outputPath = inputArgs.outputPath;
		new File(outputPath).mkdirs();
		OutputOptions outputOptions = new OutputOptions();
		outputOptions.setSetIndex(inputArgs.isGetIndexFromSourceDB());
		outputOptions.setOutputFormatValue("metadataxml");
		Path path;
		if (tableOptions.getTableInclusionRule().getInclusionPattern().toString()
				.endsWith(inputArgs.schemaName + ".*")) {
			path = Paths.get(outputPath + File.separator + "metadatatemp_" + inputArgs.getDatabase() + "_"
					+ inputArgs.getSchema() + ".xml");
			schemaTextOptions.setFullMetadataFile(true);
		} else {
			path = Paths.get(outputPath + File.separator + "metadata_selective_tablestemp_" + inputArgs.getDatabase()
					+ "_" + inputArgs.getSchema() + ".xml");
			schemaTextOptions.setFullMetadataFile(false);
		}
		outputOptions.setOutputFile(path);
		outputOptions.setSplitDate(Constants.SPLITDATE);
		schemaTextExecutable.setOutputOptions(outputOptions);
		schemaTextExecutable.setSchemaTextOptions(schemaTextOptions);
		Catalog xcatalog = SchemaCrawlerUtility.getCatalog(connection, tableOptions);
		final Catalog schemaCatalog = (Constants.METADATA_COUNT)
				? new CatalogWithCounts(xcatalog, connection, tableOptions)
				: xcatalog;
		schemaTextExecutable.executeOn(schemaCatalog, connection);

		System.out.println("Performing Row Count on tables for metadata");
		int tableCount = schemaCatalog.getTables().size();

		System.out.println("Writing rowcount details to metadata file");
		doMetadataFileFormatting(path, inputArgs.getSchema(), tableCount, schemaTextOptions.isFullMetadataFile());
	}

	private void doMetadataFileFormatting(Path path, String schema, int tableCount, boolean isFull) {
		// int flag = isFull ? 0 : 1;
		int flag = 0;
		File f = new File(path.toString());
		File w = null;
		if (isFull) {
			w = new File(path.toString().replace("metadatatemp", "metadata"));
		} else {
			w = new File(path.toString().replace("metadata_selective_tablestemp", "metadata_selective_tables"));
		}
		try {
			FileReader reader = new FileReader(f);
			FileWriter writer = new FileWriter(w);
			BufferedReader bufferedReader = new BufferedReader(reader);
			BufferedWriter bufferedWriter = new BufferedWriter(writer);
			String line;
			boolean process = true;
			while ((line = bufferedReader.readLine()) != null) {
				if (process) {
					switch (flag) {
					case 0:
						if (line.contains(DEFAULTSCHEMA)) {
							line = line.replace(DEFAULTSCHEMA, Validations.getTextFormatted(schema.toUpperCase()));
							flag++;
						}
						break;
					case 1:
						if (line.contains(NAME)) {
							line = line.replace(NAME, Validations.getTextFormatted(schema.toUpperCase()));
							flag++;
						}
						break;
					case 2:
						if (line.contains(TABLECOUNT)) {
							line = line.replace(TABLECOUNT, Integer.toString(tableCount));
							flag++;
							process = false;
						}
						break;
					default:
						break;
					}
				}
				bufferedWriter.write(line);
				bufferedWriter.write("\n");
			}
			bufferedWriter.flush();
			bufferedWriter.close();
			bufferedReader.close();
			path.toFile().delete();
			if (Constants.MOVE_FILE_TO_NAS)
				FileUtil.movefile(w.getAbsolutePath(), Constants.NAS_FILE_PATH, false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
