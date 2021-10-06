package com.p3.archon.coc.beans;

import java.io.File;
import java.util.UUID;

import org.kohsuke.args4j.Option;

import com.p3.archon.coc.utils.FileUtil;
import com.p3.archon.securitymodule.SecurityModule;

public class InputBean {

	@Option(name = "-mf", aliases = {
			"--metadatafiles" }, usage = "path to one or more metadata file seperate by semicolon", required = true)
	public String metadataFilePath;

	@Option(name = "-u", aliases = { "--user" }, usage = "ia username", required = true)
	public String infoarchiveUserName;

	@Option(name = "-p", aliases = { "--pass" }, usage = "ia password", required = true)
	public String infoarchivePassword;

	@Option(name = "-i", aliases = { "--ia" }, usage = "ia installation dir path", required = true)
	public String infoarchivePath;

	@Option(name = "-a", aliases = { "--app" }, usage = "app name", required = true)
	public String infoarchiveApplicationName;

	@Option(name = "-op", aliases = { "--outputpath" }, usage = "report output path", required = true)
	public String outputPath;

	@Option(name = "-uuid", aliases = { "--reportUUID" }, usage = "UUID for report")
	public String reportId;

	@Option(name = "-d", aliases = { "--database" }, usage = "ia database name", required = true)
	public String infoarchiveDatabaseName;

	@Option(name = "-e", aliases = { "--encrypted" }, usage = "include if password is encrypted")
	public boolean isPasswordEncrypted = false;

	@Option(name = "-logger", aliases = { "--logger" }, usage = "logger type")
	public String logger;
	
	@Option(name = "-jobName", aliases = { "--jobName" }, usage = "Job Name")
	public String jobName;

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getLogger() {
		return logger;
	}

	public void setLogger(String logger) {
		this.logger = logger;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getMetadataFiles() {
		return metadataFilePath;
	}

	public void setMetadataFiles(String metadataFiles) {
		this.metadataFilePath = metadataFiles;
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

	public String getIaPath() {
		return infoarchivePath;
	}

	public void setIaPath(String iaPath) {
		this.infoarchivePath = iaPath;
	}

	public String getAppName() {
		return infoarchiveApplicationName;
	}

	public void setAppName(String appName) {
		this.infoarchiveApplicationName = appName;
	}

	public String getDbName() {
		return infoarchiveDatabaseName;
	}

	public void setDbName(String dbName) {
		this.infoarchiveDatabaseName = dbName;
	}

	public String getReportId() {
		return reportId;
	}

	public void setReportId(String reportId) {
		this.reportId = reportId;
	}

	public boolean isEnc() {
		return isPasswordEncrypted;
	}

	public void setEnc(boolean enc) {
		this.isPasswordEncrypted = enc;
	}

	public void validate() {
		if (reportId == null)
			reportId = UUID.randomUUID().toString();

		if (infoarchiveDatabaseName == null || infoarchiveDatabaseName.isEmpty()) {
			System.err.println("Error encountered :" + "InfoArchive database value is null.");
			System.exit(1);
		}

		try {
			checkPath(metadataFilePath);
			checkiaPath(infoarchivePath);
		} catch (Exception e) {
			System.err.println("Error encountered :" + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void checkPath(String ip) throws Exception {
		String path[] = ip.split(";");
		if (path.length == 0)
			throw new Exception("Metadata file is unspecified");
		else
			for (String file : path) {
				if (!FileUtil.checkForFile(file))
					throw new Exception("File " + file + " is invalid");
			}
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
