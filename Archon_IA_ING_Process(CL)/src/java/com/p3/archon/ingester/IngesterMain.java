package com.p3.archon.ingester;

import java.io.Console;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.UUID;

import org.yaml.snakeyaml.Yaml;

import com.google.gson.Gson;
import com.p3.archon.commonfunctions.CommonFunctions;
import com.p3.archon.constants.BuildConstants;
import com.p3.archon.constants.CommonSharedConstants;
import com.p3.archon.core.ArchonDBUtils;
import com.p3.archon.dboperations.dbmodel.enums.RunMode;
import com.p3.archon.dboperations.dbmodel.enums.ToolName;
import com.p3.archon.fun.AsciiGen;
import com.p3.archon.ingester.beans.InputBean;
import com.p3.archon.ingester.jobmode.DBMode;
import com.p3.archon.ingester.jobmode.ManualMode;
import com.p3.archon.ingester.license.LicenseCheck;
import com.p3.archon.ingester.utils.FileUtil;
import com.p3.archon.securitymodule.SecurityModule;

public class IngesterMain {

	public static void main(String[] args) {
		AsciiGen.start();
		System.out.println(BuildConstants.STATUS + " " + BuildConstants.BUILD);
//		args = new String[] {"dbmode","-jid","51ac523c-979a-4b6b-97e5-3d702bff8975","-rid","0","-ja","0"};
		args = new String[] { "config" };

		if (args.length == 0) {
			System.err.println("No arguments specified.\nTerminating ... ");
			System.out.println("Job Terminated = " + new Date());
			System.exit(1);
		}

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				try {
					String tempLoc = FileUtil.createTempFolder();
					File f = new File(tempLoc);
					if (f.exists()) {
						FileUtil.deleteDirectory(tempLoc);
					}
				} catch (Exception e) {
				}
			}
		}));

		try {
			switch (RunMode.getMode(args[0])) {
			case DB:
				CommonFunctions.readUpdatePropFile();
				Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
					public void run() {
						try {
							ArchonDBUtils.closeSessionFactory();
						} catch (Exception e) {

						}
					}
				}));
				DBMode db = new DBMode(args);
				db.startValidateMode();
				break;
			case MANUAL:
				ManualMode manualMode = new ManualMode(args);
				manualMode.startValidateMode();
				break;
			case CONFIG:
				String licensePath = "";
				String configurationPath = "";
				try {
					if (args.length == 1) {
						String archonHomePath = new File(
								IngesterMain.class.getProtectionDomain().getCodeSource().getLocation().toURI())
										.getParent();
						licensePath = archonHomePath + File.separator + ".." + File.separator + "archonlicense.lic";
						configurationPath = archonHomePath + File.separator + ".." + File.separator + "config"
								+ File.separator + "configuration_iaing.yml";
					} else {
						licensePath = args[2];
						configurationPath = args[1];
					}

				} catch (URISyntaxException e1) {
					System.out.println(
							"Failed to read the license/configuration file.. please provide it as a arguments");
					e1.printStackTrace();
				}

				LicenseCheck licCheck = new LicenseCheck();
				licCheck.checkLicense(new File(licensePath));

				Yaml yaml = new Yaml();
				FileInputStream inputStream = new FileInputStream(new File(configurationPath));
				InputBean inputArgs = yaml.loadAs(inputStream, InputBean.class);
				String jsonString = new Gson().toJson(inputArgs);
				if (inputArgs.getPass() == null) {
					char password[] = null;
					do {
						System.out.println("Enter IA Password: ");
						Console c = System.console();
						password = c.readPassword();
					} while (password == null);
					inputArgs.setPass(String.valueOf(password));

				} else {
					if (inputArgs.isEnc()) {
						String decPass = SecurityModule.perfromDecrypt("5nPqgr3erA5K1P05xtuknA==",
								"fgqLxNs8tyzrW7XgQOIb7w==", inputArgs.getPass());
						inputArgs.setPass(decPass);

					}
				}

				System.out.println("Job Started = " + new Date());
				System.out.println("Starting Ingestion (" + new Date() + ")");

				String rowid = UUID.randomUUID().toString().substring(0, 8);
				String newOp = inputArgs.getOutputPath() + File.separator + ToolName.TOOL_ING_IA_NAME.getValue()
						+ File.separator
						+ (inputArgs.getJobName() == null ? "" : inputArgs.getJobName() + File.separator)
						+ CommonSharedConstants.CCYYMMDD_FORMATTER.format(new Date()) + File.separator + rowid
						+ File.separator;
				inputArgs.setOutputPath(newOp);
				System.out.println("Output Path =>" + inputArgs.getOutputPath());
				File file = new File(inputArgs.getOutputPath() + File.separator + "logs" + File.separator
						+ "archon_log_" + rowid + ".txt");
				if (inputArgs.getLogger().equalsIgnoreCase("file")) {
					FileUtil.checkCreateDirectory(inputArgs.getOutputPath() + File.separator + "logs");
					PrintStream stream = new PrintStream(file);
					System.out.println("Log Dir Path =>" + file.getParent());
					System.setErr(stream);
					System.setOut(stream);
				}
				inputArgs.setOutputLog(file.getParent() + File.separator + "output_ia_log.txt");
				inputArgs.setErrorLog(file.getParent() + File.separator + "error_ia_log.txt");
				inputArgs.setReportId(rowid);
				inputArgs.setToolName(ToolName.TOOL_ING_IA_NAME.getValue());
				CommonSharedConstants.IA_VERSION = inputArgs.getIaVersion();
				System.out.println("Arguments passed => " + jsonString);

				ManualMode config = new ManualMode(inputArgs);
				config.startValidateMode();
				System.out.println("Job Completed = " + new Date());
				break;
			default:
				System.err.println(
						"Run mode was invalid. Possible Value are\n - DBMODE\n - MANUAL\n - CONFIG\nTerminating ... ");
				System.out.println("Job Terminated = " + new Date());
				System.exit(2);
				break;
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception occured during execution. " + e.getMessage() + "\nTerminating ... ");
			System.out.println("Job Terminated = " + new Date());
			System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
			System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
			System.err.println("Exception occured during execution. " + e.getMessage() + "\nTerminating ... ");
			System.out.println("Job Terminated = " + new Date());
			System.exit(3);
		}
		System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
		System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
		System.out.println("Job Completed = " + new Date());
		System.exit(0);
	}
}
