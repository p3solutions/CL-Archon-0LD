package com.p3.archon.coreprocess;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Date;

import com.p3.archon.commonfunctions.CommonFunctions;
import com.p3.archon.constants.BuildConstants;
import com.p3.archon.core.ArchonDBUtils;
import com.p3.archon.coreprocess.jobmode.DBMode;
import com.p3.archon.coreprocess.jobmode.ManualMode;
import com.p3.archon.coreprocess.license.LicenseCheck;
import com.p3.archon.dboperations.dbmodel.enums.RunMode;
import com.p3.archon.fun.AsciiGen;

public class ArchonCore {

	public static void main(String[] args) {
		args = new String[] { "config" };

		AsciiGen.start();
		System.out.println(BuildConstants.STATUS + " " + BuildConstants.BUILD);
		if (args.length == 0) {
			System.err.println("No arguments specified.\nTerminating ... ");
			System.out.println("Job Terminated = " + new Date());
			System.exit(1);
		}

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				CommonFunctions.deleteTempDir();
			}
		}));

		try {
			switch (RunMode.getMode(args[0])) {
			case DB:
				Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
					public void run() {
						ArchonDBUtils.closeSessionFactory();
					}
				}));
				DBMode db = new DBMode(args);
				db.startValidateMode();
				break;
			case MANUAL:
				ManualMode man = new ManualMode();
				man.start((String[]) Arrays.copyOfRange(args, 1, args.length));
				break;
			case CONFIG:
				String licensePath = "";
				String configurationPath = "";
				try {
					if (args.length == 1) {
						String archonHomePath = new File(
								ArchonCore.class.getProtectionDomain().getCodeSource().getLocation().toURI())
										.getParent();
						licensePath = archonHomePath + File.separator + ".." + File.separator + "archonlicense.lic";
						configurationPath = archonHomePath + File.separator + ".." + File.separator + "config"
								+ File.separator + "configuration_dl.yml";
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
				ManualMode config = new ManualMode();
				config.startConfigMode(configurationPath);
				System.out.println("Job Completed at " + new Date());
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
			System.err.println("Exception occured during exceution." + e.getMessage() + "\nTerminating ... ");
			System.out.println("Job Terminated = " + new Date());
			System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
			System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
			System.err.println("Exception occured during exceution." + e.getMessage() + "\nTerminating ... ");
			System.out.println("Job Terminated = " + new Date());
			System.exit(3);
		}
		System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
		System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
		System.out.println("Job Completed at " + new Date());
		System.exit(0);
	}

}
