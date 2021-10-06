package com.p3.archon.ingester.beans;

import java.io.File;
import java.util.UUID;

import org.kohsuke.args4j.Option;

import com.p3.archon.ingester.utils.FileUtil;
import com.p3.archon.securitymodule.SecurityModule;

public class InputBean {

	@Option(name = "-a", aliases = { "--app" }, usage = "app name", required = true)
	public String infoarchiveApplicationName;

	@Option(name = "-s", aliases = { "--schema" }, usage = "ia schema", required = false)
	public String infoarchiveSchemaName;

	@Option(name = "-db", aliases = { "--database" }, usage = "ia database", required = false)
	public String infoarchiveDatabaseName;

	@Option(name = "-u", aliases = { "--user" }, usage = "ia username", required = true)
	public String infoarchiveUserName;

	@Option(name = "-p", aliases = { "--pass" }, usage = "ia password", required = true)
	public String infoarchivePassword;

	@Option(name = "-e", aliases = { "--encrypted" }, usage = "include if password is encrypted")
	public boolean isPasswordEncrypted = false;

	@Option(name = "-i", aliases = { "--ia" }, usage = "ia installation dir path", required = true)
	public String infoarchivePath;

	@Option(name = "-dp", aliases = { "--datapath" }, usage = "Data/Table Directory", required = true)
	public String dataPath;

	@Option(name = "-op", aliases = { "--outputpath" }, usage = "report output path", required = true)
	public String outputPath;

	@Option(name = "-uuid", aliases = { "--reportUUID" }, usage = "UUID for report")
	public String reportId;

	@Option(name = "-meta", aliases = { "--ingestmetadata" }, usage = "include if metadata need to be ingested")
	public boolean isMetadataIngestion = false;

	@Option(name = "-sip", aliases = { "--sip" }, usage = "include if archive type is sip")
	public boolean isSipApplication = false;

	@Option(name = "-el", aliases = { "--errorlogfile" }, usage = "report output path", required = false)
	public String errorLog;

	@Option(name = "-ol", aliases = { "--outputlogfile" }, usage = "report output path", required = false)
	public String outputLog;

	@Option(name = "-aip", aliases = { "--archonpath" }, usage = "archon installation path", required = false)
	public String archonPath;

	@Option(name = "-tn", aliases = { "--toolName" }, usage = "Tool Name", required = false)
	public String toolName;

	@Option(name = "-logger", aliases = { "--logger" }, usage = "logger type")
	public String logger;

	@Option(name = "-iaVersion", aliases = { "--iaVersion" }, usage = "Infoarchive Version")
	public String infoarchiveVersion;

	@Option(name = "-addExtentionAfterSuccessfulIngestion", aliases = {
			"--addExtentionAfterSuccessfulIngestion" }, usage = ".ingested extention will be added after successful ingestion")
	public boolean addExtentionAfterSuccessfulIngestion = true;
	
	@Option(name = "-jobName", aliases = { "--jobName" }, usage = "Job Name")
	public String jobName;

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getIaVersion() {
		return infoarchiveVersion;
	}

	public void setIaVersion(String iaVersion) {
		this.infoarchiveVersion = iaVersion;
	}

	public String getLogger() {
		return logger;
	}

	public void setLogger(String logger) {
		this.logger = logger;
	}

	public String getToolName() {
		return toolName;
	}

	public void setToolName(String toolName) {
		this.toolName = toolName;
	}

	public String getDbName() {
		return infoarchiveDatabaseName;
	}

	public void setDbName(String dbName) {
		this.infoarchiveDatabaseName = dbName;
	}

	public String getAppName() {
		return infoarchiveApplicationName;
	}

	public void setAppName(String appName) {
		this.infoarchiveApplicationName = appName;
	}

	public String getSchema() {
		return infoarchiveSchemaName;
	}

	public void setSchema(String schema) {
		this.infoarchiveSchemaName = schema;
	}

	public String getUser() {
		return infoarchiveUserName;
	}

	public void setUser(String user) {
		this.infoarchiveUserName = user;
	}

	public String getPass() {
		return infoarchivePassword;
	}

	public void setPass(String pass) {
		this.infoarchivePassword = pass;
	}

	public boolean isEnc() {
		return isPasswordEncrypted;
	}

	public void setEnc(boolean enc) {
		this.isPasswordEncrypted = enc;
	}

	public String getIaPath() {
		return infoarchivePath;
	}

	public void setIaPath(String iaPath) {
		this.infoarchivePath = iaPath;
	}

	public String getDataPath() {
		return dataPath;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getReportId() {
		return reportId;
	}

	public void setReportId(String reportId) {
		this.reportId = reportId;
	}

	public boolean isIngestMetadata() {
		return isMetadataIngestion;
	}

	public void setIngestMetadata(boolean ingestMetadata) {
		this.isMetadataIngestion = ingestMetadata;
	}

	public boolean isSip() {
		return isSipApplication;
	}

	public void setSip(boolean sip) {
		this.isSipApplication = sip;
	}

	public String getErrorLog() {
		return errorLog;
	}

	public void setErrorLog(String errorLog) {
		this.errorLog = errorLog;
	}

	public String getOutputLog() {
		return outputLog;
	}

	public void setOutputLog(String outputLog) {
		this.outputLog = outputLog;
	}

	public String getArchonPath() {
		return archonPath;
	}

	public void setArchonPath(String archonPath) {
		this.archonPath = archonPath;
	}

	public boolean isAddExtentionAfterSuccessfulIngestion() {
		return addExtentionAfterSuccessfulIngestion;
	}

	public void setAddExtentionAfterSuccessfulIngestion(boolean addExtentionAfterSuccessfulIngestion) {
		this.addExtentionAfterSuccessfulIngestion = addExtentionAfterSuccessfulIngestion;
	}

	public void validate() throws Exception {
		if (reportId == null)
			reportId = UUID.randomUUID().toString();
		if (!isSipApplication && (infoarchiveSchemaName == null || infoarchiveSchemaName.isEmpty()))
			throw new Exception("Schema value is null.");
		else if (!isSipApplication && (infoarchiveDatabaseName == null || infoarchiveDatabaseName.isEmpty()))
			throw new Exception("InfoArchive database value is null.");
		try {
			checkPath();
			checkiaPath(infoarchivePath);
		} catch (Exception e) {
			System.err.println("Error encountered :" + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void checkPath() throws Exception {
		File file = new File(dataPath);
		if (!file.exists())
			throw new Exception("Data directory path not exists.");
		else
			dataPath = dataPath.replace("\\", "/");
	}

	private void checkiaPath(String mf) throws Exception {
		if (!FileUtil.checkForFile(mf + File.separator + "lib" + File.separator + "sql2xq-jdbc-driver.jar")) {
			throw new Exception("InfoArchive path specified " + mf + " is invalid");
		}
	}

	public void decryptor() {
		if (isPasswordEncrypted)
			infoarchivePassword = SecurityModule.perfromDecrypt(infoarchiveApplicationName, infoarchiveUserName, infoarchivePassword);
	}
}
