package com.p3.archon.coc.process;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.UUID;

import com.p3.archon.coc.beans.InputBean;
import com.p3.archon.coc.utils.FileUtil;

public class Processor {

	Writer out;
	String path;
	private InputBean inputArgs;
	private final String TEMPLATE = "chain-of-custody --d applications/::::: APP :::::/databases/::::: DB :::::/schemas/::::: SCHEMA ::::: --file :::: IA PATH ::::/config/iashell/chain-of-custody-table.xml --table ::::: TABLE :::::";
	private final String CONNECT = "connect --u ::::: USERNAME ::::: --p ::::: PASSWORD :::::\ncd applications/::::: APP :::::\ncd ../../";

	public Processor(InputBean inputArgs) throws IOException {
		this.setInputArgs(inputArgs);
		path = FileUtil.createTempJobScheuleFolder(inputArgs.reportId) + "coc_"
				+ UUID.randomUUID().toString().substring(0, 8) + ".check";
		out = new FileWriter(path, false);
	}

	public String runJob() throws IOException {
		String tempate = prepareTemplateCocScript();
		write(getConnectLine(inputArgs.infoarchiveUserName, inputArgs.infoarchivePassword));
		String path[] = inputArgs.metadataFilePath.split(";");
		for (String file : path)
			updateFile(file, tempate);
		write(eof());
		out.flush();
		out.close();
		return this.path;
	}

	private void updateFile(String file, String tempate) {
		try {
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			boolean schemaFlag = false;
			boolean tableFlag = false;
			String schema = "";
			String table = "";
			while ((line = bufferedReader.readLine()) != null) {
				if (schemaFlag) {
					schema = line.trim().replace("<name>", "").replace("</name>", "");
					schemaFlag = false;
				} else if (tableFlag) {
					table = line.trim().replace("<name>", "").replace("</name>", "");
					write("\n");
					write("\n");
					// write("# " + schema + "." + table);
					write(tempate.replace("::::: SCHEMA :::::", schema).replace("::::: TABLE :::::", table));
					tableFlag = false;
				} else if (line.trim().equalsIgnoreCase("<schemaMetadata>"))
					schemaFlag = true;
				else if (line.trim().equalsIgnoreCase("<tableMetadata>"))
					tableFlag = true;
			}
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void write(String string) throws IOException {
		out.write(string);
		out.write("\n");
	}

	private String eof() {
		return "exit";
	}

	private String getConnectLine(String user, String pass) {
		return CONNECT.replace("::::: USERNAME :::::", user).replace("::::: PASSWORD :::::", pass)
				.replace("::::: APP :::::", inputArgs.infoarchiveApplicationName).replace("::::: DB :::::", inputArgs.infoarchiveDatabaseName);

	}

	private String prepareTemplateCocScript() {
		return TEMPLATE.replace("::::: APP :::::", inputArgs.infoarchiveApplicationName).replace(":::: IA PATH ::::", inputArgs.infoarchivePath)
				.replace("::::: DB :::::", inputArgs.infoarchiveDatabaseName);
	}

	public InputBean getInputArgs() {
		return inputArgs;
	}

	public void setInputArgs(InputBean inputArgs) {
		this.inputArgs = inputArgs;
	}

}
