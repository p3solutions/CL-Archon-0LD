package com.p3.archon.ingester.jobmode;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import org.apache.wink.json4j.JSONObject;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.p3.archon.commonfunctions.CommonFunctions;
import com.p3.archon.constants.CommonSharedConstants;
import com.p3.archon.constants.ExtractionConstants;
import com.p3.archon.dboperations.dao.JobDetailsDAO;
import com.p3.archon.dboperations.dbmodel.JobDetails;
import com.p3.archon.dboperations.dbmodel.RowId;
import com.p3.archon.ingester.beans.DbListInputBean;
import com.p3.archon.ingester.beans.InputBean;

public class DBMode {

	private DbListInputBean dbInputArgs;
	private JSONObject inputDetails;
	private JobDetails job;
	private String outputPath;
	private InputBean inputArgs;
	private String errorlog;
	private String outputlog;

	public DBMode(String[] args) throws IOException {
		this.inputArgs = new InputBean();
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

	private void setArchonAndLoadPath(String jobId) throws IOException {
		if (jobId == null || jobId.isEmpty())
			jobId = UUID.randomUUID().toString();
		String identifier = CommonSharedConstants.PREFIX_APP_INGESTER;
		errorlog = CommonSharedConstants.ARCHON_INSTALLATION_PATH + File.separator + "log" + File.separator + "error_"
				+ identifier + "_" + jobId + ".log";
		outputlog = CommonSharedConstants.ARCHON_INSTALLATION_PATH + File.separator + "log" + File.separator + "output_"
				+ identifier + "_" + jobId + ".log";
	}

	public void startValidateMode() throws Exception {
		JobDetailsDAO cdao = new JobDetailsDAO();
		RowId rowId = new RowId();
		rowId.setJobAttempt(dbInputArgs.jobAttempt);
		rowId.setJobId(dbInputArgs.jobId);
		rowId.setRunId(dbInputArgs.runId);
		job = cdao.getJobRecord(rowId);
		setArchonAndLoadPath(job.getRowIdString());
		outputPath = CommonFunctions.checkCreateOutputSaveLocation(job);
		inputDetails = new JSONObject(job.getInputDetails());
		if (job == null) {
			System.err.println("Please check jobid specified. \nNo such record exist in Database.\nTerminating ... ");
			System.out.println("Job Terminated = " + new Date());
			System.exit(11);
		}
		inputArgs.setAppName(inputDetails.getString(ExtractionConstants.IA_APPLICATION_NAME.getValue()));
		inputArgs.setUser(inputDetails.getString(ExtractionConstants.IA_USERNAME.getValue()));
		inputArgs.setPass(inputDetails.getString(ExtractionConstants.IA_USER_PASSWORD.getValue()));
		inputArgs.setEnc(true);
		inputArgs.setIaPath(CommonSharedConstants.INFOARCHIVE_INSTALLATION_PATH);
		inputArgs.setDataPath(inputDetails.getString(ExtractionConstants.DATA_PATH.getValue()));
		inputArgs.setOutputPath(outputPath);
		inputArgs.setReportId(job.getRowIdString());
		inputArgs.setToolName(job.getToolName().getValue());

		if (inputDetails.getBoolean(ExtractionConstants.INGEST_DATA.getValue())) {
			if (inputDetails.getBoolean(ExtractionConstants.APP_TYPE.getValue()))
				inputArgs.setSip(true);
			else {
				inputArgs.setDbName(inputDetails.getString(ExtractionConstants.IA_DATABASE_NAME.getValue()));
				inputArgs.setSchema(inputDetails.getString(ExtractionConstants.SCHEMA.getValue()));
			}
		} else if (inputDetails.getBoolean(ExtractionConstants.INGEST_METADATA.getValue())) {
			inputArgs.setDbName(inputDetails.getString(ExtractionConstants.IA_DATABASE_NAME.getValue()));
			inputArgs.setSchema(inputDetails.getString(ExtractionConstants.SCHEMA.getValue()));
			inputArgs.setIngestMetadata(true);
		}
		inputArgs.setArchonPath(CommonSharedConstants.ARCHON_INSTALLATION_PATH);
		inputArgs.setErrorLog(errorlog);
		inputArgs.setOutputLog(outputlog);

		// if (inputDetails.getBoolean(ExtractionConstants.INGEST_APP.getValue())) {
		// inputArgs.setIngestApp(true);
		// }
		inputArgs.decryptor();

		ManualMode manual = new ManualMode(inputArgs);
		manual.startValidateMode();
	}

}
