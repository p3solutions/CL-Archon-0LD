package com.p3.archon.coreprocess.jobmode;

import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.yaml.snakeyaml.Yaml;

import com.google.gson.Gson;
import com.p3.archon.commonutilities.PropReader;
import com.p3.archon.constants.CommonSharedConstants;
import com.p3.archon.coreprocess.beans.ArchonInputBean;
import com.p3.archon.coreprocess.common.Constants;
import com.p3.archon.coreprocess.license.LicenseCheck;
import com.p3.archon.coreprocess.process.DataLiberator;
import com.p3.archon.dboperations.dbmodel.enums.ToolName;
import com.p3.archon.securitymodule.SecurityModule;
import com.p3.archon.utils.FileUtil;

public class ManualMode {

	private ArchonInputBean inputArgs;

	public ManualMode(ArchonInputBean inputArgs) {
		this.inputArgs = inputArgs;
	}

	public ManualMode() {
		this.inputArgs = new ArchonInputBean();
	}

	public void startValidateMode() {
		// inputArgs.setCommand("quickdump");
		// inputArgs.setShowLob("true");
		// inputArgs.setOutputFormat("sip");
		// inputArgs.setConstants();
		// try {
		// inputArgs.validateArgs();
		// } catch (Exception e) {
		//
		// }

		PropReader p = new PropReader(Constants.PROP_FILE);
		Constants.ADDITIONAL_TABLE_TYPE = p.getArrayListOfString("ADDITIONAL_TABLE_TYPE");
		Constants.MOVE_FILE_TO_NAS = p.getBooleanValue("MOVE_FILE_TO_NAS", false);
		if (Constants.MOVE_FILE_TO_NAS) {
			Constants.NAS_FILE_PATH = p.getStringValue("NAS_FILE_PATH", "");
			Constants.NAS_FILE_PATH = Constants.NAS_FILE_PATH
					.concat(inputArgs.getOutputPath().replace(CommonSharedConstants.OUTPUT_BASE_PATH, ""));
			System.out.println(Constants.NAS_FILE_PATH);
			try {
				FileUtil.checkCreateDirectory(Constants.NAS_FILE_PATH);
			} catch (Exception e1) {
				e1.printStackTrace();
			}

			try {
				File file = null;
				Constants.TEMP_PATH = p.getStringValue("TEMP_PATH");
				if (Constants.TEMP_PATH.equals("")) {
					throw new Exception("TEMP_PATH property is unavailable or undefined.");
				}
				file = new File(Constants.TEMP_PATH);
				file.mkdirs();
				if (!file.exists())
					throw new Exception("Unable to create dir in temp path mentioned in variable.properties.");

				File checkfile = new File(file.getAbsolutePath() + File.separator + "tempFile" + new Date().getTime());
				checkfile.createNewFile();
				if (checkfile != null && checkfile.isFile())
					checkfile.delete();
				Constants.TEMP_PATH = file.getAbsolutePath();
			} catch (Exception e) {
				// e.printStackTrace();
				System.out.println("Setting up default TEMP location");
				Constants.TEMP_PATH = System.getProperty("java.io.tmpdir");
			}
			if (Constants.TEMP_PATH == null || Constants.TEMP_PATH.equals("")) {
				Constants.TEMP_PATH = new File("").getAbsolutePath();
			}
			String newOp = inputArgs.getOutputPath().replace(CommonSharedConstants.OUTPUT_BASE_PATH,
					Constants.TEMP_PATH);
			try {
				File file = new File(inputArgs.getOutputPath() + File.separator + "OutputFileInfo.info");
				Writer writer = new OutputStreamWriter(new FileOutputStream(file.getAbsolutePath(), true));
				writer.write("Since MOVE_FILE_TO_NAS property has been set to \"true\" ,");
				writer.write("\n");
				writer.write(" all the generated output files have been moved to \"" + Constants.NAS_FILE_PATH
						+ "\" path in NAS drive.");
				writer.flush();
				writer.close();
				file.setReadOnly();
			} catch (Exception e) {
				// TODO: handle exception
			}
			inputArgs.setOutputPath(newOp);
			if (inputArgs.getToolName().equals(ToolName.TOOL_ERT_EXTRACTOR_NAME))
				inputArgs.setGenerateSummaryReportPath(inputArgs.getOutputPath() + File.separator + "outSummary.html");
			System.out.println(inputArgs.getOutputPath());

		}

		DataLiberator dataliberator = new DataLiberator(inputArgs);
		try {
			dataliberator.start();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		if (Constants.IS_SIP_EXTRACT) {
			String blobFolder = inputArgs.getOutputPath() + File.separator + "BLOBs";

			try {
				if (FileUtil.checkForDirectory(blobFolder))
					FileUtil.deleteDirectory(blobFolder);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void start(String[] args) {
		processInputs(args, inputArgs);
		LicenseCheck licCheck = new LicenseCheck();
		licCheck.checkLicense(new File(inputArgs.getArchonLicensePath()));
		startValidateMode();
	}

	public static void processInputs(String[] args, ArchonInputBean inputArgs) {
		CmdLineParser parser = new CmdLineParser(inputArgs);
		try {
			parser.parseArgument(args);
			inputArgs.decryptor();
			inputArgs.setConstants();
			inputArgs.setAuditLevel();
			inputArgs.replacementMapSetter();
			inputArgs.validateArgs();
			inputArgs.setManualMode(true);
		} catch (CmdLineException e) {
			parser.printUsage(System.err);
			System.out.println("Please check arguments specified. \n" + e.getMessage() + "\nTerminating ... ");
			System.exit(1);
		} catch (Exception e) {
			System.out.println("INFO :");
			System.out.println(e.getMessage());
		}

		try {
			inputArgs.checkQueryUde();
		} catch (Exception e) {
			System.out.println("ERROR :");
			System.out.println(e.getMessage());
			System.exit(1);
		}

		if (inputArgs.getDatabaseServer() == null) {
			System.out.println("Database server type currently not supported. Terminating");
			System.exit(1);
		}
	}

	public void startConfigMode(String args) throws Exception {
		Yaml yaml = new Yaml();
		FileInputStream inputStream = new FileInputStream(new File(args));
		this.inputArgs = yaml.loadAs(inputStream, ArchonInputBean.class);
		inputStream.close();

		inputArgs.setToolName(ToolName.TOOL_RDBMS_EXTRACTOR_NAME);
		inputArgs.setConstants();
		inputArgs.validateArgs();
		inputArgs.setManualMode(true);
		String rowid = UUID.randomUUID().toString();
		inputArgs.setReportId(rowid.substring(0, 8));
		if (inputArgs.isDynamicOutputPath()) {
			String newOp = inputArgs.getOutputPath() + File.separator + ToolName.TOOL_RDBMS_EXTRACTOR_NAME.getValue()
					+ File.separator
					+ (inputArgs.getJobName() == null ? ""
							: inputArgs.getJobName() + File.separator)
					+ CommonSharedConstants.CCYYMMDD_FORMATTER.format(new Date()) + File.separator
					+ inputArgs.getReportId() + File.separator;
			inputArgs.setOutputPath(newOp);
		}
		DataLiberator dataliberator = null;

		String jsonString = new Gson().toJson(inputArgs);

		if (inputArgs.getPass() == null) {

			char password[] = null;
			int attempt = 0;
			boolean ex;
			do {
				ex = false;
				if (attempt > 0 && attempt < 3) {
					System.out.println("Invalid Input. ");
				}
				if (attempt == 3) {
					System.out.print("Max retry attempted. Terminating ... ");
					System.exit(1);
				}
				System.out.println("Enter DB Password: ");
				Console c = System.console();
				password = c.readPassword();

				inputArgs.setPass(String.valueOf(password));
				dataliberator = new DataLiberator(inputArgs);
				try {
					dataliberator.testConnection();
				} catch (Exception e) {
					ex = true;
					System.out.println(e.getMessage());
				}
				attempt++;

			} while (password == null || (attempt < 4 && ex));
		} else {
			if (inputArgs.isEnc()) {
				String decPass = SecurityModule.perfromDecrypt("5nPqgr3erA5K1P05xtuknA==", "fgqLxNs8tyzrW7XgQOIb7w==",
						inputArgs.getPass());
				inputArgs.setPass(decPass);

			}
		}
		System.out.println("Output Path =>" + inputArgs.getOutputPath());

		if (inputArgs.getLogger().equalsIgnoreCase("file")) {
			FileUtil.checkCreateDirectory(inputArgs.getOutputPath() + File.separator + "logs");
			File outputLog = new File(
					inputArgs.getOutputPath() + File.separator + "logs" + File.separator + "log_" + rowid + ".txt");
			PrintStream stream = new PrintStream(outputLog);
			System.out.println("Log File Path =>" + outputLog);
			System.setErr(stream);
			System.setOut(stream);
		}
		System.out.println("Arguments passed => " + jsonString);
		Constants.ADDITIONAL_TABLE_TYPE = new ArrayList<String>();
		if (Constants.IS_SIP_EXTRACT) {
			inputArgs.setBlobRefTag("ref");
		}
		dataliberator = new DataLiberator(inputArgs);
		try {
			dataliberator.start();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		if (Constants.IS_SIP_EXTRACT) {
			String blobFolder = inputArgs.getOutputPath() + File.separator + "BLOBs";

			try {
				if (FileUtil.checkForDirectory(blobFolder))
					FileUtil.deleteDirectory(blobFolder);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
