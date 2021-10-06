package com.p3.archon.coreprocess.executables;

import java.io.File;
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
import schemacrawler.tools.integration.graph.GraphExecutable;
import schemacrawler.tools.integration.graph.GraphOptions;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.utility.SchemaCrawlerUtility;

public class GraphExecutionJob extends ExecutionJob implements JobExecutable {

	public GraphExecutionJob(DataSource dataSource, Connection connection, ArchonInputBean inputArgs) {
		super(dataSource, connection, inputArgs);
	}

	@Override
	public void start() throws Exception {
		printLines(new String[] { "Graph Generator", "---------------", "" });
		final SchemaCrawlerOptions graphOptions = schemaCrawlerOptionsBuilder(inputArgs, false,
				SchemaInfoLevelBuilder.detailed(), true, false, false);

		GraphExecutable opterationExe = new GraphExecutable("graph");

		GraphOptions op = new GraphOptions();
		if (inputArgs.getGraphvizDot() != null)
			op.setDotPath(inputArgs.getGraphvizDot());
		op.setNoHeader(true);
		op.setNoInfo(true);
		op.setNoFooter(true);
		op.setShowOrdinalNumbers(true);

		OutputOptions outputOptions = new OutputOptions();
		outputOptions.setOutputFormatValue(inputArgs.outputFormat);

		Path path = null;

		if (inputArgs.outputFormat.equalsIgnoreCase("console")) {
			inputArgs.outputFormat = "pdf";
		}

		if (!inputArgs.outputFormat.equalsIgnoreCase("console")) {
			String outputPath = "Output";
			if (inputArgs.outputPath != null && !inputArgs.outputPath.equalsIgnoreCase(""))
				outputPath = inputArgs.outputPath;
			new File(outputPath).mkdirs();
			path = Paths.get(outputPath + File.separator + "Graph_" + Validations.checkValidFile(inputArgs.databaseName)
					+ "_" + Validations.checkValidFile(inputArgs.schemaName) + "." + inputArgs.outputFormat);
			outputOptions.setOutputFile(path);
		}

		opterationExe.setOutputOptions(outputOptions);
		opterationExe.setGraphOptions(op);

		final Catalog graphCatalog = SchemaCrawlerUtility.getCatalog(connection, graphOptions);
		System.out.println("Executing Graph Generation Job");
		opterationExe.executeOn(graphCatalog, connection);
		if (Constants.MOVE_FILE_TO_NAS) {
			FileUtil.movefile(path.toFile().getAbsolutePath(), Constants.NAS_FILE_PATH, false);
		}
	}
}
