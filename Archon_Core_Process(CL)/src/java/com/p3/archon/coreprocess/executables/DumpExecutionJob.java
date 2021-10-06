package com.p3.archon.coreprocess.executables;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;

import javax.sql.DataSource;

import com.p3.archon.coreprocess.beans.ArchonInputBean;
import com.p3.archon.coreprocess.beans.Statistics;
import com.p3.archon.coreprocess.common.Constants;
import com.p3.archon.coreprocess.validations.Validations;
import com.p3.archon.utils.FileUtil;

import schemacrawler.schema.Catalog;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.tools.text.operation.OperationExecutable;
import schemacrawler.tools.text.operation.OperationOptions;

public class DumpExecutionJob extends ExecutionJob implements JobExecutable {

	Table table;
	boolean version;
	SchemaCrawlerOptions tableDumpOptions;
	Catalog catalog;
	Statistics st;

	public DumpExecutionJob(DataSource dataSource, Connection connection, ArchonInputBean inputArgs) {
		super(dataSource, connection, inputArgs);
	}

	public DumpExecutionJob(Statistics st, DataSource dataSource, Connection connection, ArchonInputBean inputArgs,
			Table table, boolean version, SchemaCrawlerOptions tableDumpOptions, Catalog catalog) {
		super(dataSource, connection, inputArgs);
		this.st = st;
		this.table = table;
		this.version = version;
		this.tableDumpOptions = tableDumpOptions;
		this.catalog = catalog;
	}

	@Override
	public void start() throws Exception {
		System.out.println(rightPadding(table.getName(), 25) + " -- : " + "Processing Table ");
		OperationExecutable opterationExe = new OperationExecutable("quickdump");
		OperationOptions operationOptions = new OperationOptions();
		operationOptions.setVersion4(version);
		operationOptions.setSchema(inputArgs.getSchema());
		operationOptions.setShowUnqualifiedNames(true);
		operationOptions.setTableNameQuickDump(table.getFullName());
		operationOptions.setXmlChunkLimit(Constants.MAX_RECORD_PER_XML_FILE);
		operationOptions.setShowLobs(Constants.SHOW_LOB);
		operationOptions.setDateWithDateTime(Constants.SHOW_DATETIME);
		operationOptions.setSipExtract(Constants.IS_SIP_EXTRACT);
		opterationExe.setSchemaCrawlerOptions(tableDumpOptions);

		OutputOptions outputOptions = new OutputOptions();
		outputOptions.setSplitDate(Constants.SPLITDATE);
		outputOptions.setReplacementMap(inputArgs.getReplacementMap());
		outputOptions.setOutputFormatValue(inputArgs.outputFormat);
		Path path = null;
		String outputPath = "Output";
		if (!inputArgs.outputFormat.equalsIgnoreCase("console")) {
			if (inputArgs.outputPath != null && !inputArgs.outputPath.equalsIgnoreCase(""))
				outputPath = inputArgs.outputPath;
			new File(outputPath + File.separator + (inputArgs.outputFormat.equalsIgnoreCase("arxml") ? "Data" : ""))
					.mkdirs();
			path = Paths.get(outputPath + File.separator
					+ (inputArgs.outputFormat.equalsIgnoreCase("arxml") ? "Data" + File.separator : "")
					+ (version ? Validations.checkValidFile(inputArgs.getSchema().toUpperCase()) + "-" : "")
					+ Validations.checkValidFile(table.getName().toUpperCase()) + "."
					+ inputArgs.outputFormat.toLowerCase());

			if (inputArgs.outputFormat.equalsIgnoreCase("arxml")) {
				Path arpath = Paths.get(outputPath + File.separator + "Analysis_Report.html");
				outputOptions.setArReport(arpath);
			}
			outputOptions.setOutputFile(path);
			outputOptions.setNasFilePath(Constants.NAS_FILE_PATH);
			outputOptions.setMoveFileToNas(Constants.MOVE_FILE_TO_NAS);
			outputOptions.setBlobRefAttribute(inputArgs.getBlobRefTag());
		}

		opterationExe.setOutputOptions(outputOptions);
		opterationExe.setOperationOptions(operationOptions);
		opterationExe.executeOn(catalog, connection);
		if (inputArgs.outputFormat.endsWith("xml"))
			path.toFile().delete();
		else {
			if (Constants.MOVE_FILE_TO_NAS)
				FileUtil.movefile(path.toFile().getAbsolutePath(), Constants.NAS_FILE_PATH, false);
		}
		st.setRecordsProcessed(outputOptions.getRecordsProcessed());
		st.setSrcBlobCount(outputOptions.getSrcColumnBlobCount());
		st.setDestBlobCount(outputOptions.getDestColumnBlobCount());

		if (Constants.IS_SIP_EXTRACT) {
			try {
				SipPackager sipPackager = new SipPackager(inputArgs.getSchema(), table.getName(), inputArgs.outputPath,
						inputArgs, version, outputOptions.getDatatype());
				sipPackager.generateSip();
			} catch (Exception e) {
				e.printStackTrace();
				st.setError("SIP creation error. Reason : " + e.getMessage());
			}
		}
	}
}