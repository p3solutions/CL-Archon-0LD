package com.p3.archon.ingester.process;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Node;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSOutput;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.InputSource;

public class IngestFileHandler {

	private String outputPath;
	boolean isSip;
	private boolean addExtentionAfterSuccessfulIngestion = false;

	public IngestFileHandler(String outputpath, boolean isSip, boolean addExtentionAfterSuccessfulIngestion) {
		this.outputPath = outputpath;
		this.isSip = isSip;
		this.addExtentionAfterSuccessfulIngestion = addExtentionAfterSuccessfulIngestion;

	}

	public void startIngestionArchival() throws IOException {
		File succesDir = new File(outputPath + File.separator + "INGESTION_SUCCESS");
		File failDir = new File(outputPath + File.separator + "INGESTION_FAILED");

		Map<String, ArrayList<String>> successFileNames = new LinkedHashMap<String, ArrayList<String>>();
		Map<String, ArrayList<String>> failFileNames = new LinkedHashMap<String, ArrayList<String>>();

		File[] directoryListing;
		ArrayList<String> fileNames;
		String notificationFileName = "";

		if (succesDir.isDirectory()) {
			directoryListing = succesDir.listFiles();
			if (directoryListing != null) {
				for (File child : directoryListing) {
					fileNames = new ArrayList<String>();
					notificationFileName = child.getName()
							.substring(child.getName().lastIndexOf("SIP_"), child.getName().lastIndexOf("_CFW"))
							.replace("SIP_", "");
					if (successFileNames.containsKey(notificationFileName)) {
						fileNames = successFileNames.get(notificationFileName);
						fileNames.add(child.getName());
						successFileNames.put(notificationFileName, fileNames);

					} else {
						fileNames.add(child.getName());
						successFileNames.put(notificationFileName, fileNames);

					}

				}
			}
		}
		if (failDir.isDirectory()) {
			directoryListing = failDir.listFiles();
			if (directoryListing != null) {
				for (File child : directoryListing) {
					fileNames = new ArrayList<String>();
					notificationFileName = child.getName()
							.substring(child.getName().lastIndexOf("SIP_"), child.getName().lastIndexOf("_CFW"))
							.replace("SIP_", "");
					if (failFileNames.containsKey(notificationFileName)) {
						fileNames = failFileNames.get(notificationFileName);
						fileNames.add(child.getName());
						failFileNames.put(notificationFileName, fileNames);
					} else {
						fileNames.add(child.getName());
						failFileNames.put(notificationFileName, fileNames);

					}
				}
			}
		}

		File outputLoaction = new File(outputPath);
		File[] archivalPath = outputLoaction.listFiles();
		for (File child : archivalPath) {
			if (child.getName().contains("ArchiveResponse")) {
				createIngestFile(child, child.getName(), outputPath, successFileNames, failFileNames);
			}
		}

	}

	public static String readFile(String path) throws IOException {
		StringBuilder sb = new StringBuilder();
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
			String sCurrentLine = "";
			while ((sCurrentLine = br.readLine()) != null) {
				sb.append(sCurrentLine);
			}

		}

		return sb.toString();
	}

	private void createIngestFile(File child, String filename, String outputPath,
			Map<String, ArrayList<String>> successFileNames, Map<String, ArrayList<String>> failFileNames)
			throws IOException {

		int SuccessFileCount = 0;
		int FailFileCount = 0;
		FileInputStream fs = null;
		InputStreamReader in = null;
		BufferedReader br = null;
		StringBuffer sb = new StringBuffer();
		String textinLine;
		try {
			fs = new FileInputStream(child);
			in = new InputStreamReader(fs);
			br = new BufferedReader(in);
			while (true) {
				textinLine = br.readLine();
				if (textinLine == null)
					break;
				sb.append(textinLine);
			}
			String textToEdit1 = "<ExtractionResponse xmlns=\"urn:x-otx:eas:schema:archive:1.0\">";
			int cnt1 = sb.indexOf(textToEdit1);
			sb.replace(cnt1, cnt1 + textToEdit1.length(),
					"<Response xmlns=\"urn:x-otx:eas:schema:archive:1.0\"><ExtractionResponse>");

			String textToEdit2 = "</ExtractionResponse>";
			int cnt2 = sb.indexOf(textToEdit2);
			sb.replace(cnt2, cnt2 + textToEdit2.length(), "</ExtractionResponse><IngestionResponse>");
			sb.append("<Success_Ingest_Files>");
			if (successFileNames.keySet().contains(filename.replace("ArchiveResponse_", "").split("\\.")[0])) {
				for (String str : successFileNames.get(filename.replace("ArchiveResponse_", "").split("\\.")[0])) {
					sb.append("<File_Name>" + str + "</File_Name>\n");
					SuccessFileCount++;
				}
			}
			sb.append("</Success_Ingest_Files>");
			sb.append("<Failed_Ingest_Files>");
			if (failFileNames.keySet().contains(filename.replace("ArchiveResponse_", "").split("\\.")[0])) {
				for (String str : failFileNames.get(filename.replace("ArchiveResponse_", "").split("\\.")[0])) {
					sb.append("<File_Name>" + str + "</File_Name>\n");
					FailFileCount++;
				}
			}
			sb.append("</Failed_Ingest_Files>");
			sb.append("<Status>");
			if (FailFileCount == 0 && SuccessFileCount == 0)
				sb.append("None of the files got ingested/Failed for this Notification File");
			else if (FailFileCount == 0)
				sb.append("SUCESS");
			else if (SuccessFileCount == 0)
				sb.append("FAILED");
			else if (SuccessFileCount != 0 & FailFileCount != 0)
				sb.append("PARTIALLY INGESTED");
			sb.append("</Status>");
			sb.append("</IngestionResponse></Response>");

			fs.close();
			in.close();
			br.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			File file = new File(outputPath + File.separator + "IngestArchival_" + filename);
			FileWriter fstream = new FileWriter(file);
			BufferedWriter outobj = new BufferedWriter(fstream);

			outobj.write(format(sb.toString()));
			outobj.close();
			System.out.println("response file created----->" + file.getAbsolutePath());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public String format(String xml) {

		try {
			final InputSource src = new InputSource(new StringReader(xml));
			final Node document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(src)
					.getDocumentElement();

			final DOMImplementationRegistry registry = DOMImplementationRegistry.newInstance();
			final DOMImplementationLS impl = (DOMImplementationLS) registry.getDOMImplementation("LS");
			final LSSerializer writer = impl.createLSSerializer();

			writer.getDomConfig().setParameter("format-pretty-print", Boolean.TRUE);
			LSOutput lsOutput = impl.createLSOutput();
			lsOutput.setEncoding("UTF-8");
			Writer stringWriter = new StringWriter();
			lsOutput.setCharacterStream(stringWriter);
			writer.write(document, lsOutput);

			return stringWriter.toString();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void startIngestionResponse() throws IOException {
		int filecount = 0;
		File succesDir = new File(outputPath + File.separator + "INGESTION_SUCCESS");
		File failDir = new File(outputPath + File.separator + "INGESTION_FAILED");
		List<String> successFileNames = new ArrayList<>();
		List<String> failFileNames = new ArrayList<>();
		File[] directoryListing;
		if (succesDir.exists()) {
			directoryListing = succesDir.listFiles();
			if (directoryListing != null) {
				for (File child : directoryListing) {
					if (child.isFile()) {
						filecount++;
						String fn = addExtentionAfterSuccessfulIngestion
								? child.getName().substring(0, child.getName().indexOf(".ingested"))
								: child.getName();
						successFileNames.add(fn);
					}
				}
			}
		}
		if (failDir.exists()) {
			directoryListing = failDir.listFiles();
			if (directoryListing != null) {
				for (File child : directoryListing) {
					if (child.isFile()) {
						filecount++;
						failFileNames.add(child.getName());
					}
				}
			}
		}

		System.out.println("Ingestion Response File Creation Started");
		String newOutputPath = outputPath + File.separator + "Ingestion_Response" + ".xml";
		Writer writer = new OutputStreamWriter(new FileOutputStream(newOutputPath));
		writer.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>" + "\n");
		writer.write("<Ingestion_Response>\n");
		writer.write("<Total_File_count>" + filecount + "</Total_File_count>\n");
		writer.write("<File_count>\n");
		writer.write("<Success>" + successFileNames.size() + "</Success>\n");
		writer.write("<Failed>" + failFileNames.size() + "</Failed>\n");
		writer.write("</File_count>\n");
		writer.write("<File_List>\n");
		writer.write("<Success_File_List>\n");
		for (String str : successFileNames) {
			writer.write("<File_Name>" + str + "</File_Name>\n");
		}
		writer.write("</Success_File_List>\n");
		writer.write("<Failed_File_List>\n");
		for (String str : failFileNames) {
			writer.write("<File_Name>" + str + "</File_Name>\n");
		}
		writer.write("</Failed_File_List>\n");
		writer.write("</File_List>\n");
		writer.write("</Ingestion_Response>");
		writer.flush();
		writer.close();

		System.out.println("Ingestion Response File Creation Completed");

	}

	public void IngestedSuccessFileRename() {
		try {
			File succesDir = new File(outputPath + File.separator + "INGESTION_SUCCESS");
			File[] directoryListing;
			if (succesDir.exists()) {
				directoryListing = succesDir.listFiles();
				if (directoryListing != null) {
					for (File child : directoryListing) {
						if (!child.getName().endsWith(".ingested")) {
							File NewChildName = new File(child.getAbsolutePath() + ".ingested");
							child.renameTo(NewChildName);
						}
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
