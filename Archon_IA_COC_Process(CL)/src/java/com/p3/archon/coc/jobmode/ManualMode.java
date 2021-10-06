package com.p3.archon.coc.jobmode;

import java.io.File;
import java.util.Date;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.p3.archon.coc.beans.InputBean;
import com.p3.archon.coc.process.Processor;
import com.p3.archon.coc.process.ScriptRunner;
import com.p3.archon.coc.process.Text2Pdf;
import com.p3.archon.coc.utils.CommandLineProcess;
import com.p3.archon.coc.utils.FileUtil;
import com.p3.archon.coc.utils.OSIdentifier;

public class ManualMode {

	private InputBean inputArgs;

	public ManualMode(InputBean inputArgs) {
		this.inputArgs = inputArgs;
	}

	public ManualMode() {
	}

	public void startValidateMode() {
		try {
			Processor processor = new Processor(inputArgs);
			String path = processor.runJob();
			System.out.println(path);

			ScriptRunner sr = new ScriptRunner(path, inputArgs);
			String scriptPath = sr.runJob();
			System.out.println(scriptPath);

			CommandLineProcess clp = new CommandLineProcess();
			String[] cmdArray = OSIdentifier.checkOS() ? new String[] { scriptPath }
					: new String[] { "sh", scriptPath };
			boolean status = clp.run(cmdArray, inputArgs.infoarchiveApplicationName);

			Text2Pdf t2p = new Text2Pdf();
			String destReport = inputArgs.outputPath + java.io.File.separator + "coc_report(" + inputArgs.infoarchiveApplicationName + ")_"
					+ inputArgs.reportId.substring(0, 8) + ".pdf";
			File file = new File(destReport);
			file.getParentFile().mkdirs();
			t2p.createPdf(destReport, clp.outputLogFile, inputArgs.infoarchiveApplicationName, inputArgs.reportId);

			FileUtil.deleteFile(clp.outputLogFile);
			FileUtil.deleteFile(path);
			FileUtil.deleteFile(scriptPath);
			if (status) {
				System.out.println("Chain of Custody report generated at " + clp.outputLogFile);
				System.out.println("Chain of Custody check completed (" + new Date() + ")");
			} else {
				System.out.println("Chain of Custody check failed");
				System.err.println("Chain of Custody check failed (" + new Date() + ")");
				System.exit(1);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Chain of Custody check failed (" + new Date() + ")");
			System.exit(1);
		}

	}

	public void startManualMode(String[] args) {
		System.out.println("Starting Chain of Custody check at Table level (" + new Date() + ")");
		inputArgs = new InputBean();
		CmdLineParser parser = new CmdLineParser(inputArgs);

		try {
			parser.parseArgument(args);
			inputArgs.validate();
			inputArgs.decryptor();
		} catch (CmdLineException e) {
			parser.printUsage(System.err);
			System.err.println("Terminating ... ");
			System.out.println("Chain of Custody check terminated (" + new Date() + ")");
			System.exit(1);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Terminating ... ");
			System.out.println("Chain of Custody check terminated (" + new Date() + ")");
			System.exit(1);
		}

		startValidateMode();
	}

}
