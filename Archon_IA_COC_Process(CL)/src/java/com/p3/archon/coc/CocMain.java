package com.p3.archon.coc;

import java.io.Console;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import org.apache.wink.json4j.JSONException;
import org.yaml.snakeyaml.Yaml;

import com.google.gson.Gson;
import com.p3.archon.coc.beans.InputBean;
import com.p3.archon.coc.jobmode.DBMode;
import com.p3.archon.coc.jobmode.ManualMode;
import com.p3.archon.coc.license.LicenseCheck;
import com.p3.archon.coc.utils.FileUtil;
import com.p3.archon.commonfunctions.CommonFunctions;
import com.p3.archon.constants.BuildConstants;
import com.p3.archon.constants.CommonSharedConstants;
import com.p3.archon.core.ArchonDBUtils;
import com.p3.archon.dboperations.dbmodel.enums.RunMode;
import com.p3.archon.dboperations.dbmodel.enums.ToolName;
import com.p3.archon.fun.AsciiGen;
import com.p3.archon.securitymodule.SecurityModule;

public class CocMain {

	public static void main(String[] args) {
		AsciiGen.start();
		System.out.println(BuildConstants.STATUS + " " + BuildConstants.BUILD);
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

		switch (RunMode.getMode(args[0])) {
		case DB:
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				public void run() {
					try {
						ArchonDBUtils.closeSessionFactory();
					} catch (Exception e) {

					}
				}
			}));
			CommonFunctions.readUpdatePropFile();
			DBMode db = new DBMode(args);
			try {
				db.startValidateMode();
			} catch (JSONException e) {
				System.err.println("Json Excpetion encountered. " + e.getMessage() + "\nTerminating ... ");
				System.out.println("Job Terminated = " + new Date());
				System.exit(3);
			}
			break;
		case MANUAL:
			ManualMode mm = new ManualMode();
			mm.startManualMode((String[]) Arrays.copyOfRange(args, 1, args.length));
			break;
		case CONFIG:
			String licensePath = "";
			String configurationPath = "";
			try {
				if (args.length == 1) {
					String archonHomePath = new File(
							CocMain.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent();
					licensePath = archonHomePath + File.separator + ".." + File.separator + "archonlicense.lic";
					configurationPath = archonHomePath + File.separator + ".." + File.separator + "config"
							+ File.separator + "configuration_coc.yml";
				} else {
					licensePath = args[2];
					configurationPath = args[1];
				}

			} catch (URISyntaxException e1) {
				System.out.println("Failed to read the license/configuration file.. please provide it as a arguments");
				e1.printStackTrace();
			}

			LicenseCheck licCheck = new LicenseCheck();
			licCheck.checkLicense(new File(licensePath));

			Yaml yaml = new Yaml();
			FileInputStream inputStream = null;
			InputBean inputArgs = null;
			try {
				inputStream = new FileInputStream(new File(configurationPath));
				inputArgs = yaml.loadAs(inputStream, InputBean.class);
				inputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
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
			String rowid = UUID.randomUUID().toString();
			inputArgs.setReportId(rowid.substring(0, 8));
			String newOp = inputArgs.getOutputPath() + File.separator + ToolName.TOOL_COC_IA_NAME.getValue()
					+ File.separator + (inputArgs.getJobName() == null ? "" : inputArgs.getJobName() + File.separator)
					+ CommonSharedConstants.CCYYMMDD_FORMATTER.format(new Date()) + File.separator
					+ inputArgs.getReportId() + File.separator;
			inputArgs.setOutputPath(newOp);
			System.out.println("Output Path =>" + inputArgs.getOutputPath());
			if (inputArgs.getLogger().equalsIgnoreCase("file")) {

				try {
					FileUtil.checkCreateDirectory(inputArgs.getOutputPath() + File.separator + "logs");
				} catch (Exception e) {
					e.printStackTrace();
				}
				File file = new File(
						inputArgs.getOutputPath() + File.separator + "logs" + File.separator + "log_" + rowid + ".txt");
				PrintStream stream = null;
				try {
					stream = new PrintStream(file);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				System.out.println("Log Dir Path =>" + file.getAbsolutePath());
				System.setErr(stream);
				System.setOut(stream);
			}
			System.out.println("Arguments passed => " + jsonString);
			ManualMode config = new ManualMode(inputArgs);
			inputArgs.validate();
			config.startValidateMode();
			System.out.println("Job Completed at " + new Date());
			break;
		default:
			System.err.println(
					"Run mode was invalid. Possible Value are\n - DBMODE\n - MANUAL\n - CONFIG\nTerminating ... ");
			System.out.println("Job Terminated = " + new Date());
			System.exit(2);
			break;
		}
		System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
		System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
		System.out.println("Job Completed = " + new Date());
		System.exit(0);
	}

}
