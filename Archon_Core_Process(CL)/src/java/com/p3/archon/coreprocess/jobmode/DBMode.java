package com.p3.archon.coreprocess.jobmode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Date;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.p3.archon.commonfunctions.CommonFunctions;
import com.p3.archon.constants.CommonSharedConstants;
import com.p3.archon.constants.ExtractionConstants;
import com.p3.archon.coreprocess.beans.ArchonInputBean;
import com.p3.archon.coreprocess.beans.DbListInputBean;
import com.p3.archon.coreprocess.beans.ErtTableCompleteDetailBean;
import com.p3.archon.coreprocess.beans.ErtTableDetailsBean;
import com.p3.archon.coreprocess.beans.UnSupportedCharBean;
import com.p3.archon.coreprocess.validations.Validations;
import com.p3.archon.dboperations.dao.ErtJobDetailsDAO;
import com.p3.archon.dboperations.dao.ExtractionLocationDAO;
import com.p3.archon.dboperations.dao.ExtractionStatusDAO;
import com.p3.archon.dboperations.dao.JobDetailsDAO;
import com.p3.archon.dboperations.dao.WFDetailsDAO;
import com.p3.archon.dboperations.dbmodel.ErtJobDetails;
import com.p3.archon.dboperations.dbmodel.JobDetails;
import com.p3.archon.dboperations.dbmodel.RowId;
import com.p3.archon.dboperations.dbmodel.WFDetails;
import com.p3.archon.dboperations.dbmodel.enums.ErtJobType;
import com.p3.archon.dboperations.dbmodel.enums.ToolName;
import com.p3.archon.utils.FileUtil;

public class DBMode {

	private DbListInputBean dbInputArgs;
	private JSONObject inputDetails;
	private String outputPath;
	private String queryPath;
	private String tempQueryFile = "";
	private JobDetails jd;
	private ArchonInputBean inputArgs;

	private ErtTableDetailsBean ertTableDetailsBean;
	private ErtJobDetailsDAO ertJdao;
	private ErtJobDetails ertJd;
	private String[] tableList;

	private boolean isExtractJob;
	private String queryFolderLoc;
	private StringBuffer queryTitles = new StringBuffer();
	private static final String TEMP = "TEMP";
	private StringBuffer queries = new StringBuffer();
	private TreeMap<String, UnSupportedCharBean> unSupportedCharList;
	private String rm;

	public DBMode(String[] args) {
		CommonFunctions.readUpdatePropFile();
		this.inputArgs = new ArchonInputBean();
		dbInputArgs = new DbListInputBean();
		CmdLineParser parser = new CmdLineParser(dbInputArgs);
		try {
			parser.parseArgument((String[]) Arrays.copyOfRange(args, 1, args.length));
		} catch (CmdLineException e) {
			parser.printUsage(System.err);
			System.err.println("Please check arguments specified. \n" + e.getMessage() + "\nTerminating ... ");
			System.out.println("Job Terminated = " + new Date());
			System.exit(1);
		}
	}

	public void startValidateMode() throws Exception {
		JobDetailsDAO cdao = new JobDetailsDAO();
		ertTableDetailsBean = new ErtTableDetailsBean();
		ertJdao = new ErtJobDetailsDAO();

		RowId rowId = new RowId();
		rowId.setJobAttempt(dbInputArgs.jobAttempt);
		rowId.setJobId(dbInputArgs.jobId);
		rowId.setRunId(dbInputArgs.runId);
		jd = cdao.getJobRecord(rowId);
		if (jd == null) {
			System.err.println("Please check jobid specified. \nNo such record exist in Database.\nTerminating ... ");
			System.out.println("Job Terminated = " + new Date());
			System.exit(11);
		}

		inputArgs.setRowId(rowId);
		boolean result = new ExtractionStatusDAO().isRecordExists(jd.getRowId());
		if (result) {
			outputPath = new ExtractionLocationDAO().getOutputLoc(jd.getRowId());
			FileUtil.checkCreateDirectory(outputPath);
		} else
			outputPath = CommonFunctions.checkCreateOutputSaveLocation(jd);

		String summaryReportFile;
		inputDetails = new JSONObject(jd.getInputDetails());

		if (jd.getToolName().equals(ToolName.TOOL_ERT_EXTRACTOR_NAME)) {
			ertJd = ertJdao.getRecordsOfJob(inputDetails.getString(ExtractionConstants.JOB_ID.getValue()));
			isExtractJob = inputDetails.getBoolean(ExtractionConstants.IS_EXTRACT_JOB.getValue());
			tableList = inputDetails.getString(ExtractionConstants.TABLE_LIST.getValue()).split(",");
			setTableJobDetails();
			beginErtJob();
		}

		inputArgs.setDatabaseServer(inputDetails.getString(ExtractionConstants.DATABASESERVER.getValue()));
		inputArgs.setHost(inputDetails.getString(ExtractionConstants.HOST.getValue()));
		inputArgs.setDatabase(inputDetails.getString(ExtractionConstants.DATABASE.getValue()));
		inputArgs.setPort(inputDetails.getString(ExtractionConstants.PORT.getValue()));
		inputArgs.setSchema(inputDetails.getString(ExtractionConstants.SCHEMA.getValue()));
		inputArgs.setUser(inputDetails.getString(ExtractionConstants.USERNAME.getValue()));
		inputArgs.setPass(inputDetails.getString(ExtractionConstants.PASSWORD.getValue()));
		inputArgs.setEnc(true);
		inputArgs.setOutputPath(outputPath);
		inputArgs.setToolName(jd.getToolName());
		inputArgs.setReportId(
				(jd.getRowIdString() == null || jd.getRowIdString().isEmpty()) ? UUID.randomUUID().toString()
						: jd.getRowIdString());

		if (jd.getToolName().equals(ToolName.TOOL_ERT_EXTRACTOR_NAME)) {
			unSupportedCharList = new TreeMap<String, UnSupportedCharBean>();
			inputArgs.setOutputFormat(isExtractJob ? "xml" : "arxml");
			inputArgs.setRpx(ertTableDetailsBean.getRpx());
			inputArgs.setDateWithDatetime(String.valueOf(ertTableDetailsBean.isShowDateTime()));
			inputArgs.setMpp("3");
			inputArgs.setCommand("query");
			inputArgs.setQueryTitle(queryTitles.toString());
			inputArgs.setQueryFile("true");

//			inputArgs.setQuery(queries.toString());
			inputArgs.setSdf("false");
			inputArgs.setShowLob("true");
			if (isExtractJob) {
				summaryReportFile = outputPath + File.separator + "outSummary.html";
				inputArgs.setGenerateSummaryReportPath(summaryReportFile);
			}
			inputArgs.setReportId(jd.getRowIdString());
			processReplacementCharacter();
			inputArgs.setRm(getRm());
		}

		if (jd.getToolName().equals(ToolName.TOOL_RDBMS_EXTRACTOR_NAME)) {
			inputArgs.setOutputFormat(inputDetails.getString(ExtractionConstants.OUTPUT_FILE_FORMAT.getValue()));
			inputArgs.setTable(inputDetails.getString(ExtractionConstants.TABLE_INCLUSION_REGEX.getValue()));
			inputArgs.setIncludeRelationship(
					inputDetails.getString(ExtractionConstants.INCLUDE_RELATIONSHIP.getValue()));
			inputArgs.setRpx(inputDetails.getString(ExtractionConstants.RPX.getValue()));
			inputArgs.setMpp(inputDetails.getString(ExtractionConstants.MAX_PARALLEL_PROCESS.getValue()));
			inputArgs.setMc(inputDetails.getString(ExtractionConstants.METADATA_COUNT.getValue()));
			inputArgs.setNt(inputDetails.getString(ExtractionConstants.NEED_TABLES.getValue()));
			inputArgs.setNv(inputDetails.getString(ExtractionConstants.NEED_VIEWS.getValue()));
			inputArgs.setSdf(inputDetails.getString(ExtractionConstants.SPLIT_DATE_FIELDS.getValue()));
			inputArgs.setShowLob(inputDetails.getString(ExtractionConstants.EXTRACT_LOBS.getValue()));
			inputArgs.setDateWithDatetime(inputDetails.getString(ExtractionConstants.SHOW_ORACLEDATEWITHTIME.getValue()));
			inputArgs.setVersion("4");

			if (inputArgs.getOutputFormat().equalsIgnoreCase("sip")) {
				inputArgs
						.setApplicationName(inputDetails.getString(ExtractionConstants.EXT_SIP_IA_APP_NAME.getValue()));
				inputArgs.setHoldingPrefix(
						inputDetails.getString(ExtractionConstants.EXT_SIP_HOLDING_PREFIX.getValue()));
			}
			if (inputDetails.getString(ExtractionConstants.COMMAND.getValue()).equalsIgnoreCase("query")) {
				inputArgs.setQueryTitle(inputDetails.getString(ExtractionConstants.QUERY_TITLE.getValue()));
				inputArgs.setQueryFile(inputDetails.getString(ExtractionConstants.QUERY_FILE.getValue()));

				if (Boolean.valueOf(inputDetails.getString(ExtractionConstants.QUERY_FILE.getValue())))
					queryPath = getQueryPath(inputDetails.getString(ExtractionConstants.QUERY.getValue()));
				else
					queryPath = inputDetails.getString(ExtractionConstants.QUERY.getValue());
//				inputArgs.setQuery(queryPath);
			}
		}

		if (jd.getToolName().equals(ToolName.TOOL_BLOB_EXTRACTOR_NAME)) {
			inputArgs.setBlobIdentifier(inputDetails.getString(ExtractionConstants.BLOB_IDENTIFIER.getValue()));
			inputArgs.setRpx(inputDetails.getString(ExtractionConstants.BPF.getValue()));
			inputArgs.setUtn(inputDetails.getString(ExtractionConstants.EXTRACTION_TABLENAME.getValue()));
			inputArgs.setUbc(inputDetails.getString(ExtractionConstants.BLOB_COLUMN.getValue()));
			inputArgs.setUfo(inputDetails.getString(ExtractionConstants.FILE_NAME_OPTION.getValue()));
			if(inputDetails.containsKey(ExtractionConstants.FILE_NAME_COLUMN.getValue()))
				inputArgs.setUfc(inputDetails.getString(ExtractionConstants.FILE_NAME_COLUMN.getValue()));
			if(inputDetails.containsKey(ExtractionConstants.FILE_NAME_SEQ_START_NUMBER.getValue()))
				inputArgs.setUss(inputDetails.getString(ExtractionConstants.FILE_NAME_SEQ_START_NUMBER.getValue()));
			if(inputDetails.containsKey(ExtractionConstants.FILE_NAME_PREFIX.getValue()))
				inputArgs.setUfp(inputDetails.getString(ExtractionConstants.FILE_NAME_PREFIX.getValue()));
			inputArgs.setUeo(inputDetails.getString(ExtractionConstants.FILE_EXT_OPTION.getValue()));
			inputArgs.setUec(inputDetails.getString(ExtractionConstants.FILE_EXT_COLUMN.getValue()));
			inputArgs.setUev(inputDetails.getString(ExtractionConstants.FILE_EXT_VALUE.getValue()));
			inputArgs.setNt("true");
			inputArgs.setNv("true");
		}

		if (jd.getToolName().equals(ToolName.TOOL_CAA_JDE_NAME)
				|| jd.getToolName().equals(ToolName.TOOL_CAA_ORACLE_EBS_NAME)
				|| jd.getToolName().equals(ToolName.TOOL_CAA_PS_NAME)
				|| jd.getToolName().equals(ToolName.TOOL_CAA_SAP_NAME)) {
			inputArgs.setOutputFormat(inputDetails.getString(ExtractionConstants.OUTPUT_FILE_FORMAT.getValue()));
			inputArgs.setRpx(inputDetails.getString(ExtractionConstants.RPX.getValue()));
			inputArgs.setMpp(inputDetails.getString(ExtractionConstants.MAX_PARALLEL_PROCESS.getValue()));
			inputArgs.setCommand("dataExtract");
			inputArgs.setTable(inputDetails.getString(ExtractionConstants.TABLE_INCLUSION_REGEX.getValue()));
			inputArgs.setDateWithDatetime(inputDetails.getString(ExtractionConstants.SHOW_ORACLEDATEWITHTIME.getValue()));
		} else {
			if(!jd.getToolName().equals(ToolName.TOOL_ERT_EXTRACTOR_NAME)) {
				inputArgs.setOutputFormat(inputDetails.getString(ExtractionConstants.OUTPUT_FILE_FORMAT.getValue()));
				inputArgs.setCommand(inputDetails.getString(ExtractionConstants.COMMAND.getValue()));
			}
			inputArgs.setGraphvizDot(CommonSharedConstants.DOT_CMD);
		}

		inputArgs.decryptor();
		inputArgs.setConstants();
		inputArgs.setAuditLevel();
		inputArgs.replacementMapSetter();
		inputArgs.validateArgs();
		ManualMode manual = new ManualMode(inputArgs);
		manual.startValidateMode();
	}

	private String getQueryPath(String queryContent) throws IOException {
		String[] queryTitleTemp = inputArgs.getQueryTitle().split(";");
		String[] queryValue = queryContent.split(";");
		for (int i = 0; i < queryTitleTemp.length; i++) {
			String queryFile = CommonFunctions.createTempJobScheuleFolder(jd.getRowIdString()) + File.separator
					+ queryTitleTemp[i] + "_" + CommonSharedConstants.TEMP_QUERY_FILE;
			Writer cmd = new OutputStreamWriter(new FileOutputStream(queryFile));
			cmd.write(queryValue[i]);
			cmd.flush();
			cmd.close();
			tempQueryFile += ";" + queryFile;
		}

		return tempQueryFile.substring(1);
	}

	private void setTableJobDetails() throws JSONException {
		JSONObject obj = new JSONObject(ertJdao.getRecordsOfJob(ertJd.getJobId()).getSavedJobDetails());

		for (String table : tableList) {
			ErtTableCompleteDetailBean tableComp = new ErtTableCompleteDetailBean();
			JSONObject tableDetails = obj.getJSONObject("tables").getJSONObject(table);
			tableComp.setSelected(Boolean.valueOf(tableDetails.getString("selected")));
			tableComp.setDeleteData(Boolean.valueOf(tableDetails.getString("deleteData")));
			tableComp.setWhereOrderClause(tableDetails.getString("whereOrderClause"));
			tableComp.setQueryClause(tableDetails.getString("queryClause"));
			if (tableDetails.getString("aliasTableName") != null
					|| !tableDetails.getString("aliasTableName").equals("null"))
				tableComp.setAliasName(tableDetails.getString("aliasTableName"));
			else
				tableComp.setAliasName(table);
			ertTableDetailsBean.getTableDetails().put(table, tableComp);
		}
		ertTableDetailsBean.setRpx(obj.getString("rpx"));
		ertTableDetailsBean.setShowDateTime(Boolean.valueOf(obj.getString("showdatetime")));
		ertTableDetailsBean.setIngestionAppName(obj.getString("ingestionAppName"));
		ertTableDetailsBean.setSchemaName(obj.getString("schemaName"));
		ertTableDetailsBean.setUsername(obj.getString("username"));
		ertTableDetailsBean.setPassword(obj.getString("password"));
		ertTableDetailsBean.setDeleteData(Boolean.valueOf(obj.getString("deleteData")));
		ertTableDetailsBean.setIngestData(Boolean.valueOf(obj.getString("ingestData")));
	}

	public boolean beginErtJob() {
		boolean status = false;
		try {
			queryFolderLoc = CommonFunctions.createTempJobScheuleFolder(jd.getRowIdString());
			queryTitles = new StringBuffer();
			String server = inputDetails.getString(ExtractionConstants.DATABASESERVER.getValue());
			String schema = inputDetails.getString(ExtractionConstants.SCHEMA.getValue());

			switch (ertJd.getJobType()) {
			case TABLE:
				for (String tb : ertTableDetailsBean.getTableDetails().keySet()) {
					if (ertJd.getJobType() == ErtJobType.TABLE
							&& !ertTableDetailsBean.getTableDetails().get(tb).isSelected())
						continue;
					else if (ertJd.getJobType() != ErtJobType.TABLE && !ertTableDetailsBean.getMainTable().equals(tb))
						continue;
					// mainTable = tb;
					ErtTableCompleteDetailBean tc = ertTableDetailsBean.getTableDetails().get(tb);
					String filel = queryFolderLoc + File.separator + tc.getAliasName() + ".sql";
					Writer out = new OutputStreamWriter(new FileOutputStream(filel));
					String deleteFile = outputPath + File.separator + TEMP + File.separator + "IDS" + File.separator
							+ Validations.getTextFormatted(tb) + ".ids";
					if (isExtractJob && !new File(deleteFile).exists()) {
						new File(outputPath + File.separator + TEMP + File.separator + "IDS").mkdirs();
						new File(deleteFile).createNewFile();
					}
					String sql = "";
					if (server.toUpperCase().equals("SQL")) {
						sql = "Select " + tc.getQueryClause() + " from " + (schema + "." + tb) + " "
								+ tc.getWhereOrderClause();

					} else {
						sql = "Select " + tc.getQueryClause().replace("'", "\"") + " from " + (schema + "." + tb) + " "
								+ tc.getWhereOrderClause();
					}
					if (ertJd.getJobType() == ErtJobType.TABLE && isExtractJob && ertTableDetailsBean.isDeleteData()
							&& ertTableDetailsBean.getTableDetails().get(tb).isDeleteData()) {
						Writer deleteDataWriter = new OutputStreamWriter(new FileOutputStream(deleteFile));
						deleteDataWriter.write("delete " + sql.substring(sql.indexOf("from " + schema + "." + tb)));
						deleteDataWriter.flush();
						deleteDataWriter.close();
					}

					out.write(sql);
					out.flush();
					out.close();

					if (queryTitles.toString().equals(""))
						queryTitles.append(ertTableDetailsBean.getTableDetails().get(tb).getAliasName());
					else
						queryTitles.append(";").append(ertTableDetailsBean.getTableDetails().get(tb).getAliasName());

					if (queries.toString().equals(""))
						queries.append(new File(filel).getAbsolutePath());
					else
						queries.append(";").append(new File(filel).getAbsolutePath());

					if (ertJd.getJobType() != ErtJobType.TABLE)
						break;
				}
				break;
			default:
				break;
			}
			status = true;
		} catch (Exception e) {
			e.printStackTrace();
			status = false;
			// jobMessage.append("<br/>").append(e.getMessage());
			return status;
		}
		return status;
	}

	private void processReplacementCharacter() throws JSONException {
		WFDetailsDAO wfdao = new WFDetailsDAO();
		WFDetails wf = wfdao.getRecordById(new ErtJobDetailsDAO()

				.getRecordsOfJob(inputDetails.getString(ExtractionConstants.JOB_ID.getValue())).getWfId());
		String replacementChar = wf.getReplacementMap();
		if (replacementChar != null && replacementChar.isEmpty()) {
			String str = "{" + '"' + "REPLACEMENT_MAP" + '"' + ":";
			unSupportedCharList = toMap(
					new JSONArray(replacementChar.substring(str.length(), replacementChar.length())));
		}

	}

	private String getRm() {
		for (Entry<String, UnSupportedCharBean> x : unSupportedCharList.entrySet()) {
			rm = (x.getKey() + ":,:" + x.getValue().getReplacementChar() + ":;:").replace("\"", "\\\"");
		}
		return rm;
	}

	public static TreeMap<String, UnSupportedCharBean> toMap(JSONArray replacementChar) throws JSONException {
		TreeMap<String, UnSupportedCharBean> map = new TreeMap<String, UnSupportedCharBean>();
		String key = "REPLACEMENT_MAP";
		for (int i = 0; i < replacementChar.size(); i++) {
			UnSupportedCharBean value = new UnSupportedCharBean();
			JSONObject object = new JSONObject(replacementChar.get(i));
			value.setCodePoint(object.getString("CP"));
			value.setDateUpdated(object.getString("UD"));
			value.setReplacementChar(object.getString("RC"));
			map.put(key, value);
		}
		return map;
	}

}
