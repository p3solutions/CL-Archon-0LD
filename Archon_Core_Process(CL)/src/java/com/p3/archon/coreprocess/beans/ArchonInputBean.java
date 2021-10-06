package com.p3.archon.coreprocess.beans;

import java.util.TreeMap;
import java.util.UUID;
import java.util.logging.Level;

import org.kohsuke.args4j.Option;

import com.p3.archon.coreprocess.common.Constants;
import com.p3.archon.dboperations.dbmodel.RowId;
import com.p3.archon.dboperations.dbmodel.enums.ToolName;
import com.p3.archon.securitymodule.SecurityModule;

public class ArchonInputBean {

	private static final int DEFAULT_RPX = 100;

	private boolean bypasscommand = false;

	@Option(name = "-dbs", aliases = {
			"--databaseserver" }, usage = "database server type (SQL, Oracle, MySQL, MARIA, TERADATA, DB2, PostgreSQL, Sybase ...)", required = true)
	public String databaseServer;

	@Option(name = "-h", aliases = { "--host" }, usage = "host Server IP", required = true)
	public String hostName;

	@Option(name = "-l", aliases = { "--port" }, usage = "database listening port", required = true)
	public String portNumber;

	@Option(name = "-d", aliases = { "--database" }, usage = "database name", required = true)
	public String databaseName;

	@Option(name = "-s", aliases = { "--schema" }, usage = "schema name", required = true)
	public String schemaName;

	@Option(name = "-u", aliases = { "--user" }, usage = "database username", required = true)
	public String userName;

	@Option(name = "-r", aliases = { "--relationoship" }, usage = "include Relationship details in metadata file")
	public String includeRelationship;

	@Option(name = "-p", aliases = { "--pass" }, usage = "database password", required = true)
	public String password;

	@Option(name = "-c", aliases = {
			"--command" }, usage = "command to execute (schemageneration, dataExtract, quickdump, count, query, metadata, unstructured)")
	public String command;

	@Option(name = "-o", aliases = {
			"--outputFormat" }, usage = "Result output format (txt, csv, tsv, html, xml, console, sip)", depends = {
					"-op" })
	public String outputFormat;

	@Option(name = "-op", aliases = {
			"--outputPath" }, usage = "Destination folder to save output. Value will be ignore if output format is set as console.")
	public String outputPath;

	@Option(name = "-t", aliases = { "--table" }, usage = "table name")
	public String tableInclusionRule;

	@Option(name = "-queryType", aliases = { "--queryType" }, usage = " Options include: ( queryText, queryFile )")
	public String queryType;

	@Option(name = "-qt", aliases = { "--querytitle" }, usage = "title for query")
	public String queryTitle;

	@Option(name = "-qText", aliases = { "--queryText" }, usage = "Query Text")
	public String queryText;

	@Option(name = "-queryFile", aliases = { "--queryFile" }, usage = "Query File")
	public String queryFile;

	@Option(name = "-v", aliases = {
			"--infoversion" }, usage = "output format for infoarchive version. if value is 3.2, then output will be for infoarchive 3.2, any other value will set it to default infoarchive 4")
	public String version;

	@Option(name = "-rpx", aliases = { "--recrdsperxml" }, usage = "Records per XML size in MB (default value : "
			+ DEFAULT_RPX + ")")
	public String outputFileSplitSize;

	@Option(name = "-mpp", aliases = { "--maxparallelprocess" }, usage = "Max Parallel process (default value : 3)")
	public String maximumParallelProcess;

	@Option(name = "-sdf", aliases = {
			"--splitdatefields" }, usage = "Split Date Fields (for XML compatability) (default value : true)")
	public String generateXMLCompatibleDateTimeFields;

	@Option(name = "-nt", aliases = {
			"--needtables" }, usage = "include \"tables\" in operation performed (default value : true)")
	public String includeTable;

	@Option(name = "-nv", aliases = {
			"--needviews" }, usage = "include \"view\" in operation performed (default value : false)")
	public String includeView;

	@Option(name = "-mc", aliases = {
			"--metadatacount" }, usage = "Inclde count in metdata file (default value : false)")
	public String includeRecordCount;

	@Option(name = "-ueo", aliases = {
			"--extoption" }, usage = "true/false : true if extension is to be got from column in table (default value : false)")
	public String ueo;

	@Option(name = "-ufo", aliases = {
			"--filenameoption" }, usage = "Can be \"\" or \"COLUMN\" or \"SEQNAME\" (default selection : \"\")")
	public String ufo;

	@Option(name = "-uss", aliases = {
			"--filenameseqstartnumber" }, usage = "Define the sequence starting number for naming files to be extracted. Must be integer (default start value : 1)")
	public String uss;

	@Option(name = "-ufp", aliases = {
			"--filenameprefix" }, usage = "Define the prefix to be used in naming files to be extracted (default prefix : \"\")")
	public String ufp;

	@Option(name = "-ufc", aliases = {
			"--filenamecolumn" }, usage = "Column name in the table to get name for files to be extracted")
	public String ufc;

	@Option(name = "-uec", aliases = {
			"--fileextcolumn" }, usage = "Column name in the table to get fileextension for files to be extracted")
	public String uec;

	@Option(name = "-uev", aliases = {
			"--fileextvalue" }, usage = "Define the fileextension for files to be extracted (Default extension : \"\")")
	public String uev;

	@Option(name = "-ubc", aliases = {
			"--blobcolumn" }, usage = "Column name in the table to get blob date for extraction")
	public String ubc;

	@Option(name = "-utn", aliases = {
			"--extractiontablename" }, usage = "Table name from which unstructred data must be extracted")
	public String utn;

	@Option(name = "-sl", aliases = {
			"--showlob" }, usage = "should lob be extracted as part of data or not (true/false). Default value false. This is appliable only for XML type extractions")
	public String includeLobContent;

	@Option(name = "-a", aliases = {
			"--auditlevel" }, usage = "OFF, ALL, SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST  (default value : OFF)")
	public String audit;

	@Option(name = "-bi", aliases = {
			"--blobidentifier" }, usage = "Identifier for Blob folder during parallel extraction (Default : X)")
	public String blobIdentifier;

	@Option(name = "-rm", aliases = { "--replacementmap" }, usage = "Replacement Map for XML unsupported chars")
	public String rm;

	@Option(name = "-sr", aliases = {
			"--generatesummaryreportrath" }, usage = "Path where summary report is to be generated")
	public String generateSummaryReportPath;

	@Option(name = "-uuid", aliases = { "--reportUUID" }, usage = "UUID for report (ERT only)")
	public String reportId;

	@Option(name = "-gv", aliases = { "--dotloc" }, usage = "Location for dot file (Grphviz)")
	public String graphvizDotPath;

	@Option(name = "-e", aliases = { "--encrypted" }, usage = "include if password is encrypted")
	public boolean isPasswordEncrypted = false;

	@Option(name = "-mm", aliases = {
			"--manualmode" }, usage = "manual mode representing the inputs provided in command line")
	public boolean manualMode = false;

	@Option(name = "-san", aliases = { "--sipappname" }, usage = "SIP Application Name")
	public String applicationName;

	@Option(name = "-shp", aliases = { "--sipholdingprefix" }, usage = "Holding Name Prefix for SIP packages")
	public String holdingPrefix;

	@Option(name = "-dt", aliases = { "--datetiem" }, usage = "Extract datetime as part of data")
	public String extractDateWithTimeFormat;

	@Option(name = "-tn", aliases = { "--toolName" }, usage = "Tool Name")
	public ToolName toolName;

	@Option(name = "-brt", aliases = {
			"--additonalTableType" }, usage = "Blob Reference Attribute (default value : ref)")
	public String blobReferenceAttribute = "ref";

	@Option(name = "-lp", aliases = { "--archonLicensePath" }, usage = "licence File Path")
	public String archonLicensePath;

	@Option(name = "-dop", aliases = { "--dynamicOutputPath" }, usage = "dynamicOutputPath")
	public boolean dynamicOutputPath;

	@Option(name = "-gi", aliases = { "--getIndexFromSourceDB" }, usage = "getIndexFromSourceDB")
	public boolean getIndexFromSourceDB;

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

	public boolean isGetIndexFromSourceDB() {
		return getIndexFromSourceDB;
	}

	public void setGetIndexFromSourceDB(boolean getIndexFromSourceDB) {
		this.getIndexFromSourceDB = getIndexFromSourceDB;
	}

	public String getLogger() {
		return logger;
	}

	public void setLogger(String logger) {
		this.logger = logger;
	}

	public boolean isDynamicOutputPath() {
		return dynamicOutputPath;
	}

	public void setDynamicOutputPath(boolean dynamicOutputPath) {
		this.dynamicOutputPath = dynamicOutputPath;
	}

	public String getArchonLicensePath() {
		return archonLicensePath;
	}

	public void setArchonLicensePath(String archonLicensePath) {
		this.archonLicensePath = archonLicensePath;
	}

	public String getBlobRefTag() {
		return blobReferenceAttribute;
	}

	public void setBlobRefTag(String blobRefTag) {
		this.blobReferenceAttribute = blobRefTag;
	}

	public ToolName getToolName() {
		return toolName;
	}

	public void setToolName(ToolName toolName2) {
		this.toolName = toolName2;
	}

	public String getApplicationName() {
		return applicationName;
	}

	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}

	public String getHoldingPrefix() {
		return holdingPrefix;
	}

	public void setHoldingPrefix(String holdingPrefix) {
		this.holdingPrefix = holdingPrefix;
	}

	public RowId rowId;

	public RowId getRowId() {
		return rowId;
	}

	public void setRowId(RowId rowId) {
		this.rowId = rowId;
	}

	public boolean isManualMode() {
		return manualMode;
	}

	public void setManualMode(boolean manualMode) {
		this.manualMode = manualMode;
	}

	public boolean isEnc() {
		return isPasswordEncrypted;
	}

	public void setEnc(boolean enc) {
		this.isPasswordEncrypted = enc;
	}

	public String getGraphvizDot() {
		return graphvizDotPath;
	}

	public String getQueryType() {
		return queryType;
	}

	public void setQueryType(String queryType) {
		this.queryType = queryType;
	}

	public String getQueryText() {
		return queryText;
	}

	public void setQueryText(String queryText) {
		this.queryText = queryText;
	}

	public void setGraphvizDot(String graphvizDot) {
		this.graphvizDotPath = graphvizDot;
	}

	public String getGenerateSummaryReportPath() {
		return generateSummaryReportPath;
	}

	public void setGenerateSummaryReportPath(String generateSummaryReportPath) {
		this.generateSummaryReportPath = generateSummaryReportPath;
	}

	public String getReportId() {
		return reportId;
	}

	public void setReportId(String reportId) {
		this.reportId = reportId;
	}

	public String getRm() {
		return rm;
	}

	public void setRm(String rm) {
		this.rm = rm;
	}

	public String getBlobIdentifier() {
		return blobIdentifier;
	}

	public void setBlobIdentifier(String blobIdentifier) {
		this.blobIdentifier = blobIdentifier;
	}

	public String getNt() {
		return includeTable;
	}

	public void setNt(String nt) {
		this.includeTable = nt;
	}

	public String getNv() {
		return includeView;
	}

	public void setNv(String nv) {
		this.includeView = nv;
	}

	public String getShowLob() {
		return includeLobContent;
	}

	public void setShowLob(String showLob) {
		this.includeLobContent = showLob;
	}

	public String getIncludeRelationship() {
		return includeRelationship;
	}

	public void setIncludeRelationship(String includeRelationship) {
		this.includeRelationship = includeRelationship;
	}

	public String getUeo() {
		return ueo;
	}

	public void setUeo(String ueo) {
		this.ueo = ueo;
	}

	public String getUfo() {
		return ufo;
	}

	public void setUfo(String ufo) {
		this.ufo = ufo;
	}

	public String getUss() {
		return uss;
	}

	public void setUss(String uss) {
		this.uss = uss;
	}

	public String getUfp() {
		return ufp;
	}

	public void setUfp(String ufp) {
		this.ufp = ufp;
	}

	public String getUfc() {
		return ufc;
	}

	public void setUfc(String ufc) {
		this.ufc = ufc;
	}

	public String getUec() {
		return uec;
	}

	public void setUec(String uec) {
		this.uec = uec;
	}

	public String getUev() {
		return uev;
	}

	public void setUev(String uev) {
		this.uev = uev;
	}

	public String getUbc() {
		return ubc;
	}

	public void setUbc(String ubc) {
		this.ubc = ubc;
	}

	public String getUtn() {
		return utn;
	}

	public void setUtn(String utn) {
		this.utn = utn;
	}

	public String getAudit() {
		return audit;
	}

	public void setAudit(String audit) {
		this.audit = audit;
	}

	public String getRpx() {
		return outputFileSplitSize;
	}

	public void setRpx(String rpx) {
		this.outputFileSplitSize = rpx;
	}

	public String getMpp() {
		return maximumParallelProcess;
	}

	public void setMpp(String mpp) {
		this.maximumParallelProcess = mpp;
	}

	public String getSdf() {
		return generateXMLCompatibleDateTimeFields;
	}

	public void setSdf(String sdf) {
		this.generateXMLCompatibleDateTimeFields = sdf;
	}

	public String getMc() {
		return includeRecordCount;
	}

	public void setMc(String mc) {
		this.includeRecordCount = mc;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getHost() {
		return hostName;
	}

	public void setHost(String host) {
		this.hostName = host;
	}

	public String getUser() {
		return userName;
	}

	public void setUser(String user) {
		this.userName = user;
	}

	public String getPass() {
		return password;
	}

	public void setPass(String pass) {
		this.password = pass;
	}

	public String getCommand() {
		return command;
	}

	public void setCommand(String command) {
		this.command = command;
	}

	public String getOutputFormat() {
		return outputFormat;
	}

	public void setOutputFormat(String outputFormat) {
		this.outputFormat = outputFormat;
	}

	public void setAuditLevel() {
		if (audit != null) {
			switch (audit.toUpperCase().trim()) {
			case "ALL":
				System.out.println("Logger level : ALL");
				Constants.LOG_LEVEL = Level.ALL;
				break;
			case "SEVERE":
				System.out.println("Logger level : SEVERE");
				Constants.LOG_LEVEL = Level.SEVERE;
				break;
			case "WARNING":
				System.out.println("Logger level : WARNING");
				Constants.LOG_LEVEL = Level.WARNING;
				break;
			case "INFO":
				System.out.println("Logger level : INFO");
				Constants.LOG_LEVEL = Level.INFO;
				break;
			case "CONFIG":
				System.out.println("Logger level : CONFIG");
				Constants.LOG_LEVEL = Level.CONFIG;
				break;
			case "FINE":
				System.out.println("Logger level : FINE");
				Constants.LOG_LEVEL = Level.FINE;
				break;
			case "FINER":
				System.out.println("Logger level : FINER");
				Constants.LOG_LEVEL = Level.FINER;
				break;
			case "FINEST":
				System.out.println("Logger level : FINEST");
				Constants.LOG_LEVEL = Level.FINEST;
				break;
			case "OFF":
				System.out.println("Logger level : OFF");
				Constants.LOG_LEVEL = Level.OFF;
				break;
			default:
				System.out.println("Audit type parameter is invalid. setting default value.\nLogger level : OFF");
				Constants.LOG_LEVEL = Level.OFF;
			}
		} else {
			System.out.println("Logger level : OFF");
			Constants.LOG_LEVEL = Level.OFF;
		}
	}

	public void setConstants() {
		if (outputFileSplitSize != null && !manualMode) {
			if (toolName.equals(ToolName.TOOL_BLOB_EXTRACTOR_NAME)) {
				if (!outputFileSplitSize.equals("")) {
					try {
						Constants.MAX_RECORD_PER_XML_FILE = Integer.parseInt(outputFileSplitSize);
					} catch (Exception e) {
						Constants.MAX_RECORD_PER_XML_FILE = DEFAULT_RPX;
					}
				} else
					Constants.MAX_RECORD_PER_XML_FILE = DEFAULT_RPX;
			} else {
				if (!outputFileSplitSize.equals("")) {
					try {
						Constants.MAX_RECORD_PER_XML_FILE = Integer.parseInt(outputFileSplitSize) * 1024 * 1024;
					} catch (Exception e) {
						Constants.MAX_RECORD_PER_XML_FILE = DEFAULT_RPX * 1024 * 1024;
					}
				} else
					Constants.MAX_RECORD_PER_XML_FILE = DEFAULT_RPX * 1024 * 1024;
			}

		} else {
			if (!outputFileSplitSize.equals("")) {
				try {
					Constants.MAX_RECORD_PER_XML_FILE = Integer.parseInt(outputFileSplitSize) * 1024 * 1024;
				} catch (Exception e) {
					Constants.MAX_RECORD_PER_XML_FILE = DEFAULT_RPX * 1024 * 1024;
				}
			} else
				Constants.MAX_RECORD_PER_XML_FILE = DEFAULT_RPX * 1024 * 1024;
		}
		if (maximumParallelProcess != null) {
			if (!maximumParallelProcess.equals("")) {
				try {
					Constants.MAX_SCHEMA_PARALLEL_EXT = Integer.parseInt(maximumParallelProcess);
					Constants.MAX_TABLES_PARALLEL_EXT = Integer.parseInt(maximumParallelProcess);
				} catch (Exception e) {
					Constants.MAX_SCHEMA_PARALLEL_EXT = 3;
					Constants.MAX_TABLES_PARALLEL_EXT = 3;
				}
			} else {
				Constants.MAX_SCHEMA_PARALLEL_EXT = 3;
				Constants.MAX_TABLES_PARALLEL_EXT = 3;
			}
		}
		if (includeTable != null) {
			if (!includeTable.equals("")) {
				Constants.NEED_TABLES = includeTable.equalsIgnoreCase("false") ? false : true;
			} else
				Constants.NEED_TABLES = true;
		}

		if (includeView != null) {
			if (!includeView.equals("")) {
				Constants.NEED_VIEWS = includeView.equalsIgnoreCase("true") ? true : false;
			} else
				Constants.NEED_VIEWS = false;
		}
		if (generateXMLCompatibleDateTimeFields != null) {
			if (!generateXMLCompatibleDateTimeFields.equals("")) {
				Constants.SPLITDATE = generateXMLCompatibleDateTimeFields.equalsIgnoreCase("true") ? true : false;
			} else
				Constants.SPLITDATE = false;
		}
		if (includeRecordCount != null) {
			if (!includeRecordCount.equals("")) {
				Constants.METADATA_COUNT = includeRecordCount.equalsIgnoreCase("true") ? true : false;
			} else
				Constants.METADATA_COUNT = true;
		}
		if (includeLobContent != null) {
			if (!includeLobContent.equals("")) {
				Constants.SHOW_LOB = includeLobContent.equalsIgnoreCase("true") ? true : false;
			} else
				Constants.SHOW_LOB = false;
		}
		if (extractDateWithTimeFormat != null) {
			if (!extractDateWithTimeFormat.equals("")) {
				Constants.SHOW_DATETIME = extractDateWithTimeFormat.equalsIgnoreCase("true") ? true : false;
			} else
				Constants.SHOW_DATETIME = false;
		}
	}

	public void validateArgs() throws Exception {
		StringBuffer sb = new StringBuffer();
		sb.append(checkServer());
		sb.append(checkCommand());
		sb.append(checkQuery());
		sb.append(checkOutputFormat());
		sb.append(checkVersion());

		if (!command.trim().toLowerCase().equals("quickdump") && !command.trim().toLowerCase().equals("dataextract")
				&& !command.trim().toLowerCase().equals("query") && outputFormat.trim().toLowerCase().equals("sip")) {
			String message = "\t" + "Output format set '" + outputFormat
					+ "' is not a supported option for the command choosen. Setting default output format (xml)."
					+ "\n";
			System.out.println(message);
			setOutputFormat("xml");
		}

		if (outputFormat.trim().toLowerCase().equals("sip")) {
			if (applicationName == null || applicationName.equals("") || holdingPrefix == null
					|| holdingPrefix.equals(""))
				throw new Exception(
						"For output format 'sip', the following inputs are mandatory \n\t'SIP Application Name'\n\t'Holding Prefix'.\nEither one or both are missing. \nTerminating ...");
			setOutputFormat("xml");
			Constants.IS_SIP_EXTRACT = true;
		} else {
			Constants.IS_SIP_EXTRACT = false;
		}

		if (!sb.toString().equals(""))
			throw new Exception(sb.toString());

		if (reportId == null || reportId.isEmpty())
			reportId = UUID.randomUUID().toString();
	}

	public void checkQueryUde() throws Exception {
		if (command.equalsIgnoreCase("query"))
			if (outputFormat.equalsIgnoreCase("file"))
				if ((ubc == null) || (ubc.equals("")))
					throw new Exception(
							"For 'query' mode with output type 'file', parameter -ubc holding the blob content column name is mantatory. \nTerminating ...");
	}

	public FileExtInputBean checkUnstructuredInputs() throws Exception {
		// Default Values
		boolean fileNameExtOption = false;
		String fileNameOption = "";
		String fileNamePrefixSeq = "";
		String fileNameColumn = "";
		String fileNameExtColumn = "";
		String fileNameExtValue = "";
		String blobColumn = "";
		String tableName = "";
		int fileNameStartSeq = 1;

		fileNameExtOption = (ueo == null) ? false : ueo.equalsIgnoreCase("true") ? true : false;
		fileNameOption = (ufo == null) ? ""
				: ufo.equalsIgnoreCase("COLUMN") ? "COLUMN" : ufo.equalsIgnoreCase("SEQNAME") ? "SEQNAME" : "";
		try {
			fileNameStartSeq = Integer.parseInt(uss);
		} catch (Exception e) {
			fileNameStartSeq = 1;
		}

		fileNamePrefixSeq = (ufp == null) ? "" : ufp;
		fileNameColumn = (ufc == null) ? "" : ufc;
		fileNameExtColumn = (uec == null) ? "" : uec;
		fileNameExtValue = (uev == null) ? "" : uev;
		blobColumn = (ubc == null) ? "" : ubc;
		tableName = (utn == null) ? "" : utn;

		StringBuffer mainerror = new StringBuffer();
		mainerror.append("Terminating...");
		if (tableName.equals(""))
			mainerror.append("\n\tInput for unstructred table name parameter (-utn) is blank or unspecified.");
		if (blobColumn.equals(""))
			mainerror.append("\n\tInput for blob column name parameter (-ubc) is blank or unspecified.");
		if (fileNameExtOption && fileNameExtColumn.equals(""))
			mainerror.append(
					"\n\tOption to get Extension from column (-ueo) was true but paramter defining the column to get Extension from (-uec) was blank or unspecified.");
		if (fileNameOption.equals("COLUMN") && fileNameColumn.equals(""))
			mainerror.append(
					"\n\tOption to get filename from column (-ufo) was \"COLUMN\" but paramter defining the column to get filename from (-ufc) was blank or unspecified.");
		if (fileNameOption.equals("SEQNAME") && fileNamePrefixSeq.equals(""))
			mainerror.append(
					"\n\tOption to get filename from column (-ufo) was \"SEQNAME\" but entry for file name prefix (-ufp) is blank or unspecified.");

		if (!mainerror.toString().endsWith("Terminating..."))
			throw new Exception(mainerror.toString());

		FileExtInputBean feib = new FileExtInputBean();

		feib.setBlobColumn(blobColumn);
		feib.setFileNameColumn(fileNameColumn);
		feib.setFileNameExtColumn(fileNameExtColumn);
		feib.setFileNameExtOption(fileNameExtOption);
		feib.setFileNameExtValue(fileNameExtValue);
		feib.setFileNameOption(fileNameOption);
		feib.setFileNamePrefixSeq(fileNamePrefixSeq);
		feib.setFileNameStartSeq(fileNameStartSeq);
		feib.setTableName(tableName);

		return feib;

	}

	private String checkVersion() {
		if (version != null) {
			switch (getVersion().trim()) {
			case "3.2":
			case "4":
				return "";
			default: {
				String message = "\t" + "Version parameter input '" + version
						+ "' is invalid. Setting default version 4." + "\n";
				version = "4";
				return message;
			}
			}
		}
		version = "4";
		return "";
	}

	private String checkOutputFormat() {
		if (outputFormat == null) {
			outputFormat = "console";
			return "\t" + "No output format specified. Setting output format as " + outputFormat + "\n";
		}
		if (command.trim().equalsIgnoreCase("dbinfo")) {
			switch (outputFormat.trim().toLowerCase()) {
			case "all":
			case "basic":
			case "date":
				return "";
			default:
				String message = "\t" + "Output format set '" + outputFormat
						+ "' is not a supported option for the command choosen. Setting default output format (basic)."
						+ "\n";
				outputFormat = "pdf";
				return message;
			}
		} else if (command.trim().equalsIgnoreCase("graph")) {
			switch (outputFormat.trim().toLowerCase()) {
			case "pdf":
			case "png":
			case "jpeg":
			case "jpg":
			case "bmp":
			case "dot":
				return "";
			default:
				String message = "\t" + "Output format set '" + outputFormat
						+ "' is not a supported option for the command choosen. Setting default output format (pdf)."
						+ "\n";
				outputFormat = "pdf";
				return message;
			}
		} else {
			switch (outputFormat.trim().toLowerCase()) {
			case "csv":
			case "tsv":
			case "txt":
			case "xml":
			case "arxml":
			case "html":
			case "json":
			case "console":
			case "file":
			case "sip":
				return "";
			default:
				String message = "\t" + "Output format set '" + outputFormat
						+ "' is not a supported option for the command choosen. Setting default output format (console)."
						+ "\n";
				outputFormat = "console";
				return message;
			}
		}
	}

	private String checkQuery() {
		if (!bypasscommand) {
			if (command.equalsIgnoreCase("query")) {

				if (queryType.equalsIgnoreCase("queryText")) {

					if (((queryText == null || queryText.trim().equals(""))))
						return "queryType is set to queryText and queryText is not provided";

				} else {
					if ((queryFile == null || queryFile.trim().equals("")))
						return "queryType is set to queryFile and queryFile is not provided";
				}
			}
		}
		return "";
	}

	private String checkCommand() {
		switch (command.trim().toLowerCase()) {
		case "schemageneration":
		case "schema":
		case "dataextract":
		case "quickdump":
		case "count":
		case "test":
		case "query":
		case "graph":
		case "unstructured":
		case "dbinfo":
		case "dblinkquery":
			return "";
		case "metadata":
			if (includeRelationship != null && includeRelationship.trim().equalsIgnoreCase("true"))
				includeRelationship = "true";
			else
				includeRelationship = "false";
			return "";
		default: {
			String message = "\t" + "Command input '" + command
					+ "' is invalid. Setting default command (schemageneration)" + "\n";
			setBypasscommand(true);
			command = "schemageneration";
			return (message);
		}
		}
	}

	private String checkServer() {
		switch (databaseServer.trim().toLowerCase()) {
		case "teradata":
		case "sql":
		case "sqlwinauth":
		case "oracle":
		case "oracleservice":
		case "mysql":
		case "maria":
		case "mariadb":
		case "db2":
		case "postgresql":
		case "sybase":
		case "as400":
		case "as400noport":
			return "";
		default: {
			String message = "\t" + "Server " + databaseServer
					+ " not supported. Supported server types 'SQL', 'ORACLE', 'MySQL', 'DB2', 'POSTGRESQL', 'SYBASE', 'MARIA', 'TERADATA'"
					+ "\n";
			databaseServer = null;
			return message;
		}
		}
	}

	public boolean isBypasscommand() {
		return bypasscommand;
	}

	public void setBypasscommand(boolean bypasscommand) {
		this.bypasscommand = bypasscommand;
	}

	public String getDatabase() {
		return databaseName;
	}

	public void setDatabase(String database) {
		this.databaseName = database;
	}

	public String getSchema() {
		return schemaName;
	}

	public void setSchema(String schema) {
		this.schemaName = schema;
	}

	public String getPort() {
		return portNumber;
	}

	public void setPort(String port) {
		this.portNumber = port;
	}

	public String getQueryTitle() {
		return queryTitle;
	}

	public void setQueryTitle(String queryTitle) {
		this.queryTitle = queryTitle;
	}

	public String getDatabaseServer() {
		return databaseServer;
	}

	public void setDatabaseServer(String databaseServer) {
		this.databaseServer = databaseServer;
	}

	public String getTable() {
		return tableInclusionRule;
	}

	public void setTable(String table) {
		this.tableInclusionRule = table;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getQueryFile() {
		return queryFile;
	}

	public void setQueryFile(String queryFile) {
		this.queryFile = queryFile;
	}

	public String getDateWithDatetime() {
		return extractDateWithTimeFormat;
	}

	public void setDateWithDatetime(String dateWithDatetime) {
		this.extractDateWithTimeFormat = dateWithDatetime;
	}

	public void printInputs() {
		System.out.println("Inputs");
		System.out.println("------");
		System.out.println();
		System.out.println("Database Details");
		System.out.println("\tServer" + " :\t " + databaseServer);
		System.out.println("\tHost" + " :\t " + hostName);
		System.out.println("\tPort" + " :\t " + portNumber);

		System.out.println("\tDatabase" + " :\t " + databaseName);
		System.out.println("\tSchema" + " :\t " + schemaName);

		System.out.println("\tUsername" + " :\t " + userName);
		System.out.println("\tPassword" + " :\t " + "******");

		System.out.println();
		System.out.println("Command Details");
		System.out.println("\tCommand" + " :\t " + command);
		System.out.println("\tTable Inclusion" + " :\t " + (tableInclusionRule == null ? "" : tableInclusionRule));
		System.out.println("\t" + (command.equalsIgnoreCase("dbinfo") ? "Analysis Mode" : "Output Format") + ":\t "
				+ outputFormat);
		System.out.println("\tSave Path" + " :\t " + outputPath);
		System.out.println("\tInfoArchive Version" + " :\t " + version);
		System.out.println("\tInclude Relationship" + " :\t " + includeRelationship);

		System.out.println();
		System.out.println("Job Properties Details (for internal use)");
		System.out.println("\tMPP" + " :\t " + maximumParallelProcess);
		System.out.println("\tRPX" + " :\t " + outputFileSplitSize);
		System.out.println("\tSDF" + " :\t " + generateXMLCompatibleDateTimeFields);
		System.out.println("\tNV" + " :\t " + includeView);
		System.out.println("\tMC" + " :\t " + includeRecordCount);
	}

	public TreeMap<String, String> replacementMap;

	public TreeMap<String, String> getReplacementMap() {
		return replacementMap;
	}

	public void setReplacementMap(TreeMap<String, String> replacementMap) {
		this.replacementMap = replacementMap;
	}

	public void replacementMapSetter() {
		replacementMap = new TreeMap<String, String>();
		if (rm != null && !rm.equals("")) {
			String[] splits = rm.split(":;:");
			for (String row : splits) {
				String[] r = row.split(":,:");
				if (r.length == 2)
					replacementMap.put(r[0], r[1]);
			}
		}
	}

	public void decryptor() {
		if (isPasswordEncrypted)
			password = SecurityModule.perfromDecrypt(databaseName, userName, password);
	}

}
