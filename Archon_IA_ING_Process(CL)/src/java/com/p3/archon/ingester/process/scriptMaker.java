package com.p3.archon.ingester.process;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import com.p3.archon.constants.CommonSharedConstants;
import com.p3.archon.ingester.beans.InputBean;

public class scriptMaker {

	private InputBean inputArgs;
	private final String CONNECT_TEMPLATE = "connect --u ::::: USERNAME ::::: --p ::::: PASSWORD :::::\ncd applications/::::: APP :::::\ncd ../../\n";
	private final String SIP_INGEST_TEMPLATE = "ingest --d applications/::::: APP ::::: --from ::::: DATA_PATH :::::/*.zip";
	private final String TABLE_INGEST_TEMPLATE = "ingest --d applications/::::: APP :::::/databases/::::: DB :::::/schemas/::::: SCHEMA ::::: --from ::::: DATA_PATH ::::: --filter \"::::: SCHEMA :::::-.*\\.xml\"";
	private final String SCRIPT_TEMPLATE = "::::: IA_PATH ::::: --cmdfile ::::: SCRIPT_PATH ::::: --cmdfile_echo";

	private final String INGEST_METADATA_TEMPLATE = "ingest-table-metadata applications/::::: APP :::::/databases/::::: DB ::::: --path ::::: DATA_PATH :::::"; // --filter
																																								// \"metadata.*\\.xml\"

	public scriptMaker(InputBean inputArgs) {
		this.inputArgs = inputArgs;
	}

	public void createScript(String path) throws IOException {
		Writer out = new FileWriter(path, false);
		out.write(getConnectTemplate());
		if (inputArgs.isIngestMetadata())
			out.write(getMetadataScriptTemplate());
		else
			out.write(getIngestTemplate());
		out.write("\nquit");
		out.flush();
		out.close();
	}

	private String getConnectTemplate() {
		return CONNECT_TEMPLATE.replace("::::: USERNAME :::::", inputArgs.getUser())
				.replace("::::: PASSWORD :::::", inputArgs.getPass()).replace("::::: APP :::::", inputArgs.infoarchiveApplicationName);
	}

	private String getIngestTemplate() {
		if (inputArgs.isSip())
			if (CommonSharedConstants.IA_VERSION.equals("16EP6") || CommonSharedConstants.IA_VERSION.equals("16EP7")
					|| CommonSharedConstants.IA_VERSION.equals("20.2")
					|| CommonSharedConstants.IA_VERSION.equals("20.4")
					|| CommonSharedConstants.IA_VERSION.equals("21.2")) {
				return SIP_INGEST_TEMPLATE.replace("::::: APP :::::", inputArgs.infoarchiveApplicationName)
						.replace("::::: DATA_PATH :::::", inputArgs.getDataPath().replace("\\", "/"))
						+ " --moveOnSuccess --moveOnSuccessTo " + inputArgs.getDataPath() + File.separator
						+ "INGESTION_SUCCESS" + " --moveOnErrorTo " + inputArgs.getDataPath() + File.separator
						+ "INGESTION_FAILED" + "";
			} else
				return SIP_INGEST_TEMPLATE.replace("::::: APP :::::", inputArgs.infoarchiveApplicationName)
						.replace("::::: DATA_PATH :::::", inputArgs.getDataPath().replace("\\", "/"));
		else
			return TABLE_INGEST_TEMPLATE.replace("::::: APP :::::", inputArgs.infoarchiveApplicationName)
					.replace(":::: IA PATH ::::", inputArgs.infoarchivePath).replace("::::: SCHEMA :::::", inputArgs.getSchema())
					.replace("::::: DATA_PATH :::::", inputArgs.getDataPath().replace("\\", "/"))
					.replace("::::: DB :::::", inputArgs.getDbName());
	}

	public void createScriptRunner(String path, String scriptPath) throws IOException {
		Writer out = new FileWriter(path, false);
		out.write(getScriptTemplate(scriptPath));
		out.flush();
		out.close();
	}

	private String getScriptTemplate(String scriptPath) {
		return SCRIPT_TEMPLATE
				.replace("::::: IA_PATH :::::",
						(inputArgs.getIaPath() + File.separator + "bin" + File.separator + "iashell"))
				.replace("::::: SCRIPT_PATH :::::", scriptPath);
	}

	private String getMetadataScriptTemplate() {
		return INGEST_METADATA_TEMPLATE.replace("::::: APP :::::", inputArgs.infoarchiveApplicationName)
				.replace(":::: IA PATH ::::", inputArgs.infoarchivePath).replace("::::: SCHEMA :::::", inputArgs.getSchema())
				.replace("::::: DATA_PATH :::::", inputArgs.getDataPath().replace("\\", "/"))
				.replace("::::: DB :::::", inputArgs.getDbName());
	}

}
