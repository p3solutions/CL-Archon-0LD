package com.p3.archon.ingester.jobmode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.p3.archon.constants.CommonSharedConstants;
import com.p3.archon.dboperations.dbmodel.enums.ToolName;
import com.p3.archon.ingester.beans.InputBean;
import com.p3.archon.ingester.process.IngestFileHandler;
import com.p3.archon.ingester.process.Text2Pdf;
import com.p3.archon.ingester.process.scriptMaker;
import com.p3.archon.ingester.utils.CommandLineProcess;
import com.p3.archon.ingester.utils.FileUtil;
import com.p3.archon.ingester.utils.OSIdentifier;

public class ManualMode {

	private InputBean inputArgs;

	public ManualMode(String args[]) {
		inputArgs = new InputBean();
		CmdLineParser parser = new CmdLineParser(inputArgs);
		try {
			parser.parseArgument((String[]) Arrays.copyOfRange(args, 1, args.length));
			inputArgs.validate();
			inputArgs.decryptor();
		} catch (CmdLineException e) {
			parser.printUsage(System.err);
			System.err.println("Terminating ... ");
			System.out.println("InfoArchive Data Ingestion terminated (" + new Date() + ")");
			System.exit(1);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Terminating ... ");
			System.out.println("InfoArchive Data Ingestion terminated (" + new Date() + ")");
			System.exit(1);
		}

	}

	public ManualMode(InputBean inputArgs) {
		this.inputArgs = inputArgs;
	}

	public void startValidateMode() {
		String ingestionStatus;
		try {
			String scriptPath = FileUtil.createTempJobScheuleFolder(inputArgs.getReportId()) + "ingest_"
					+ inputArgs.getReportId() + ".check";
			String scriptRunnerPath = FileUtil.createTempJobScheuleFolder(inputArgs.getReportId()) + "scriptRunner_"
					+ inputArgs.getReportId() + (OSIdentifier.checkOS() ? ".bat" : ".sh");
			System.out.println(scriptPath);
			System.out.println(scriptRunnerPath);
			scriptMaker sm = new scriptMaker(inputArgs);
			sm.createScript(scriptPath);
			sm.createScriptRunner(scriptRunnerPath, scriptPath);

			CommandLineProcess clp = new CommandLineProcess(inputArgs);
			String[] cmdArray = OSIdentifier.checkOS() ? new String[] { scriptRunnerPath }
					: new String[] { "sh", scriptRunnerPath };
			clp.run(cmdArray, inputArgs.infoarchiveApplicationName);

			Text2Pdf t2p = new Text2Pdf();
			String destReport = inputArgs.outputPath + File.separator + "Ingestion_report(" + inputArgs.infoarchiveApplicationName + ")_"
					+ inputArgs.getReportId().substring(0, 8) + ".pdf";
			File file = new File(destReport);
			file.getParentFile().mkdirs();
			t2p.createPdf(destReport, clp.outputLogFile, inputArgs.infoarchiveApplicationName, inputArgs.reportId);

			// FileUtil.deleteFile(clp.outputLogFile);
			FileUtil.deleteFile(scriptRunnerPath);
			FileUtil.deleteFile(scriptPath);
			ingestionStatus = checkLog(inputArgs.outputLog);
			boolean failStatus = false;
			if (ingestionStatus.startsWith("<p>Ingestion Completed Successfully.</p>")) {
				System.out.println("Ingestion report generated at " + destReport);
				System.out.println("Ingestion completed (" + new Date() + ")");
			} else {
				System.out.println("Ingestion Failed " + ingestionStatus);
				System.err.println(ingestionStatus);
				failStatus = true;
				// throw new Exception("Ingestion Failed " + ingestionStatus);
			}
			IngestFileHandler ingestHandler = new IngestFileHandler(inputArgs.getDataPath(), inputArgs.isSip(),
					inputArgs.isAddExtentionAfterSuccessfulIngestion());

			try {
				if (inputArgs.getToolName().equals(ToolName.TOOL_XML_FILE_EXTRACTOR_NAME.getValue())) {
					ingestHandler.startIngestionArchival();
					if ((CommonSharedConstants.IA_VERSION.equals("16EP6")
							|| CommonSharedConstants.IA_VERSION.equals("16EP7")
							|| CommonSharedConstants.IA_VERSION.equals("20.2")
							|| CommonSharedConstants.IA_VERSION.equals("20.4")
							|| CommonSharedConstants.IA_VERSION.equals("21.2")) && inputArgs.isSip())
						if (inputArgs.isAddExtentionAfterSuccessfulIngestion())
							ingestHandler.IngestedSuccessFileRename();
				} else {

					if ((CommonSharedConstants.IA_VERSION.equals("16EP6")
							|| CommonSharedConstants.IA_VERSION.equals("16EP7")
							|| CommonSharedConstants.IA_VERSION.equals("20.2")
							|| CommonSharedConstants.IA_VERSION.equals("20.4")
							|| CommonSharedConstants.IA_VERSION.equals("21.2")) && inputArgs.isSip())
						if (inputArgs.isAddExtentionAfterSuccessfulIngestion())
							ingestHandler.IngestedSuccessFileRename();
					if (!inputArgs.isIngestMetadata())
						ingestHandler.startIngestionResponse();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

			if (failStatus) {
				throw new Exception("Ingestion Failed " + ingestionStatus);
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	public String checkLog(String outputLog) {
		if (outputLog == null || !new File(outputLog).exists())
			return "Unknown Error";
		int count = 0;
		String returnMsg = "";
		FileReader fr = null;
		BufferedReader br = null;
		try {
			fr = new FileReader(outputLog);
			br = new BufferedReader(fr);
			String line;
			boolean start = false;
			while ((line = br.readLine()) != null) {
				if (!start && line.contains("Ingestion Report Process Log"))
					start = true;
				if (start) {
					if (line.trim().startsWith("Ingested") || line.trim().startsWith("Received")
							|| line.trim().contains("Ingested file")) // ingested with
						// attachement
						count++;
					if (line.contains("Ingest took"))
						returnMsg = "<p>Ingestion Completed Successfully.</p><p>Ingested " + count + " files.</p><p>"
								+ (line.replace("Ingest took", "Total ingestion time : ") + "</p>");
					else if (line.contains("Sorry, password you entered is incorrect, try again."))
						returnMsg = "Credentials are invalid";
					else if (line.contains("Please check application name."))
						returnMsg = "Incorrect Application name, Please check application name.";
					else if (line.contains("Cannot find files to be ingested"))
						returnMsg = "Either Schema name provide for ingestion is incorrect or no xml file found with provided schema to ingest.";
					else if (line.contains("The filename, directory name, or volume label syntax is incorrect.")
							|| line.contains("You have specified unknown directory"))
						returnMsg = "Incorrect Application name / Schema Name / wrong directory path..(Application Name and Schema Name are case sensitive)";
					else if (line.contains("Connection refused"))
						returnMsg = "Connection to Infoarchive server refused.";
					else if (line.contains("ERROR - Cannot upload the file:"))
						returnMsg = "Invalid Ingestion file";
					else if (line.contains("ERROR - Command failed: connect --u"))
						returnMsg = "Cannot decrypt a value. Either the configuration or the value is wrong";
				}
			}
			br.close();
			fr.close();
		} catch (Exception e) {
			e.printStackTrace();
			try {
				br.close();
				fr.close();
				returnMsg = returnMsg + "\n\n" + e.getMessage();
			} catch (IOException e1) {
				e1.printStackTrace();
				returnMsg = returnMsg + "\n\n" + e1.getMessage();
			}
		}
		returnMsg = returnMsg.isEmpty() ? (count == 0 ? "Unknown error" : "<p>Ingestion Completed Successfully.</p>")
				: returnMsg;
		return returnMsg;
	}
}
