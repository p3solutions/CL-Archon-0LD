package com.p3.archon.coc.jobmode;

import java.util.Arrays;
import java.util.Date;

import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.p3.archon.coc.beans.DbListInputBean;
import com.p3.archon.coc.beans.InputBean;
import com.p3.archon.commonfunctions.CommonFunctions;
import com.p3.archon.constants.CommonSharedConstants;
import com.p3.archon.constants.ExtractionConstants;
import com.p3.archon.dboperations.dao.JobDetailsDAO;
import com.p3.archon.dboperations.dbmodel.JobDetails;
import com.p3.archon.dboperations.dbmodel.RowId;

public class DBMode {

	private DbListInputBean dbInputArgs;
	private JSONObject inputDetails;
	private JobDetails jd;
	private String outputPath;
	private InputBean inputArgs;

	public DBMode(String[] args) {
		this.inputArgs = new InputBean();
		dbInputArgs = new DbListInputBean();
		// CommonFunctions.readUpdatePropFile();
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

	public void startValidateMode() throws JSONException {
		JobDetailsDAO cdao = new JobDetailsDAO();
		RowId rowId = new RowId();
		rowId.setJobAttempt(dbInputArgs.jobAttempt);
		rowId.setJobId(dbInputArgs.jobId);
		rowId.setRunId(dbInputArgs.runId);
		jd = cdao.getJobRecord(rowId);

		outputPath = CommonFunctions.checkCreateOutputSaveLocation(jd);
		inputDetails = new JSONObject(jd.getInputDetails());
		if (jd == null) {
			System.err.println("Please check jobid specified. \nNo such record exist in Database.\nTerminating ... ");
			System.out.println("Job Terminated = " + new Date());
			System.exit(1);
		}
		inputArgs.setAppName(inputDetails.getString(ExtractionConstants.IA_APPLICATION_NAME.getValue()));
		inputArgs.setUser(inputDetails.getString(ExtractionConstants.IA_USERNAME.getValue()));
		inputArgs.setDbName(inputDetails.getString(ExtractionConstants.IA_DATABASE_NAME.getValue()));
		inputArgs.setPass(inputDetails.getString(ExtractionConstants.IA_USER_PASSWORD.getValue()));
		inputArgs.setIaPath(CommonSharedConstants.INFOARCHIVE_INSTALLATION_PATH);
		inputArgs.setMetadataFiles(inputDetails.getString(ExtractionConstants.METAFILES.getValue()));
		inputArgs.setOutputPath(outputPath);
		inputArgs.reportId = jd.getRowIdString();
		inputArgs.isPasswordEncrypted = true;
		inputArgs.decryptor();
		inputArgs.validate();

		ManualMode manual = new ManualMode(inputArgs);
		manual.startValidateMode();
	}

}
