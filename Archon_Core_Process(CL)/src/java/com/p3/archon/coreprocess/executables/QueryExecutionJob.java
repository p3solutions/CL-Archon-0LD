package com.p3.archon.coreprocess.executables;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.sql.DataSource;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import com.p3.archon.coreprocess.beans.ArchonInputBean;
import com.p3.archon.coreprocess.beans.Statistics;
import com.p3.archon.coreprocess.common.Constants;
import com.p3.archon.coreprocess.validations.Validations;
import com.p3.archon.dboperations.dao.ExtractionLocationDAO;
import com.p3.archon.dboperations.dao.ExtractionStatusDAO;
import com.p3.archon.dboperations.dbmodel.ExtractionLocation;
import com.p3.archon.dboperations.dbmodel.ExtractionStatus;
import com.p3.archon.utils.FileUtil;

import schemacrawler.schema.Catalog;
import schemacrawler.schemacrawler.Config;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaInfoLevelBuilder;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.tools.text.operation.OperationExecutable;
import schemacrawler.tools.text.operation.OperationOptions;
import schemacrawler.utility.SchemaCrawlerUtility;

public class QueryExecutionJob extends ExecutionJob implements JobExecutable {

	public QueryExecutionJob(DataSource dataSource, Connection connection, ArchonInputBean inputArgs) {
		super(dataSource, connection, inputArgs);
	}

	int maxThread = Constants.MAX_TABLES_PARALLEL_EXT;
	int counter = 0;
	Map<String, Statistics> statistics = new LinkedHashMap<String, Statistics>();
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S");
	Date startTime = new Date();
	List<String> extractedTables = new ArrayList<>();
	List<ExtractionStatus> extractionStatus;
	File extractionTableInfoCsvFile;

	@SuppressWarnings("deprecation")
	@Override
	public void start() throws Exception {
		printLines(new String[] { "DataPull", "--------", "" });
		boolean version = inputArgs.getVersion().trim().equals("4");
		final SchemaCrawlerOptions queryDumpOptions = schemaCrawlerOptionsBuilder(inputArgs, false,
				SchemaInfoLevelBuilder.standard(), false, false, false);
		StringBuffer queryBuffer = new StringBuffer();
		// if (inputArgs.queryFile.equalsIgnoreCase("true")) {
		String[] queryTitles = inputArgs.queryTitle.split(";");
		String[] queriesTest = null;

		if (inputArgs.queryType.equalsIgnoreCase("queryFile")) {

			for (String fn : inputArgs.queryFile.split(";")) {

				FileReader r = new FileReader(fn);
				BufferedReader bufferedReader = new BufferedReader(r);
				String line;
				StringBuffer sb = new StringBuffer();
				while ((line = bufferedReader.readLine()) != null) {
					sb.append(" ").append(line);
				}
				r.close();
				bufferedReader.close();

				queriesTest = sb.toString().split(";");

			}

		} else {
			queriesTest = inputArgs.queryText.split(";");
		}

		String[] queries = queriesTest;

		String[] ubc = (inputArgs.outputFormat.equalsIgnoreCase("file") ? inputArgs.ubc.split(";") : null);
		Statistics st;
		if (inputArgs.isManualMode()) {
			extractionTableInfoCsvFile = new File(inputArgs.outputPath + File.separator + "ExtractedTables.csv");
			if (!extractionTableInfoCsvFile.exists()) {
				FileUtil.checkCreateDirectory(extractionTableInfoCsvFile.getParent());
				extractionTableInfoCsvFile.createNewFile();
				FileUtil.writeFileAppend(extractionTableInfoCsvFile.getAbsolutePath(),
						"Count_Match,Destination_Record,End_Time,Source_Record,Start_Time,Table_Name,Query_Title,Blobinfo"
								+ "\n");
			} else {
				FileReader filereader = new FileReader(extractionTableInfoCsvFile.getAbsolutePath());
				CSVParser parser = new CSVParserBuilder().withSeparator(',')
						.withFieldAsNull(CSVReaderNullFieldIndicator.BOTH).withIgnoreLeadingWhiteSpace(true)
						.withIgnoreQuotations(false).withQuoteChar('"').withStrictQuotes(false).build();
				CSVReader csvReader = new CSVReaderBuilder(filereader).withCSVParser(parser).build();
				String[] record;
				int iteration = 0;
				while ((record = csvReader.readNext()) != null) {

					iteration++;
					if (iteration <= 1)
						continue;
					extractedTables.add(record[5]);
					st = new Statistics();
					st.setStatus(record[0]);
					st.setRecordsProcessed(Integer.parseInt(record[1]));
					st.setEndTime(new Date((Long.parseLong(record[2]))));
					st.setSourceRecords(record[3]);
					st.setStartTime(new Date((Long.parseLong(record[4]))));

					String[] blobsinfo = (record[7] == null || record[7].equals("null") || record[7].equals("")
							? new String[] {}
							: record[7].split(SPLIT));
					st.setSrcBlobCount(new TreeMap<String, Long>());
					st.setDestBlobCount(new TreeMap<String, Long>());
					for (String string : blobsinfo) {
						String[] info = string.split(SEPERATOR);
						st.getSrcBlobCount().put(info[0], Long.parseLong(info[1]));
						st.getDestBlobCount().put(info[0], Long.parseLong(info[2]));
					}

					statistics.put(record[5], st);
					queryBuffer.append(record[5] + ":QUERY:" + record[6] + "\n");

				}
				csvReader.close();
				filereader.close();
			}
		} else {
			ExtractionStatusDAO esd = new ExtractionStatusDAO();
			boolean result = new ExtractionStatusDAO().isRecordExists(inputArgs.getRowId());
			if (result) {
				extractionStatus = esd.getJobRecord(inputArgs.getRowId());
				for (ExtractionStatus extractionStatus : extractionStatus) {
					extractedTables.add(extractionStatus.getTableName());
					st = new Statistics();
					st.setStartTime(extractionStatus.getStartTime());
					st.setEndTime(extractionStatus.getEndTime());
					st.setSourceRecords(extractionStatus.getSourceRecord());
					st.setRecordsProcessed(extractionStatus.getDestRecord());
					st.setStatus(extractionStatus.getCountMatch());
					queryBuffer
							.append(extractionStatus.getTableName() + ":QUERY:" + extractionStatus.getQuery() + "\n");
					String[] blobsinfo = ((extractionStatus.getBlobinfo() != null)
							? extractionStatus.getBlobinfo().split(SPLIT)
							: new String[] {});
					st.setSrcBlobCount(new TreeMap<String, Long>());
					st.setDestBlobCount(new TreeMap<String, Long>());
					for (String string : blobsinfo) {
						String[] info = string.split(SEPERATOR);
						st.getSrcBlobCount().put(info[0], Long.parseLong(info[1]));
						st.getDestBlobCount().put(info[0], Long.parseLong(info[2]));
					}
					statistics.put(extractionStatus.getTableName(), st);

				}

			} else {
				ExtractionLocationDAO eld = new ExtractionLocationDAO();
				ExtractionLocation el = new ExtractionLocation();
				el.setRowId(inputArgs.getRowId());
				el.setLoc(inputArgs.getOutputPath());
				eld.addRecord(el);
			}
		}
		for (int query = 0; query < queries.length; query++) {
			String table = queryTitles[query];
			if (!extractedTables.contains(table)) {
				while (counter == maxThread) {
					Thread.sleep(1000);
				}
				final int i = query;
				Thread t = new Thread(new Runnable() {
					public void run() {
						try {
							doJob(queryTitles[i], queries[i], queryDumpOptions, version,
									(inputArgs.outputFormat.equalsIgnoreCase("file") ? ubc[i] : null), queryBuffer);
							counter--;
						} catch (Exception e) {
							e.printStackTrace();
							counter--;
						}
					}
				});
				counter++;
				t.start();
				Thread.sleep(1000);
			} else
				System.out.println(rightPadding(table, 25) + " -- : " + "Table is Processed. Skipped...");
		}
		// } else {
		// doJob(inputArgs.queryTitle, inputArgs.query, queryDumpOptions, version,
		// inputArgs.schema, inputArgs.ubc, queryBuffer);
		// }

		while (counter != 0)
			Thread.sleep(50);

		printLines(new String[] { "", "", "Statistics", "----------", "",
				"Tot Extraction Time: " + timeDiff(new Date().getTime() - startTime.getTime()), "" });

		StringBuffer sb = new StringBuffer();
		StringBuffer error = new StringBuffer();
		StringBuffer blobinfo = new StringBuffer();
		boolean writeerror = false;
		boolean writeblobinfo = false;
		viewFormatter(prepareObjectArray(7), true);
		viewFormatter(new Object[] { " TABLE/TITLE", " START TIME", " END TIME",
				leftPadding("SOURCE RECORDS COUNT", 22), leftPadding("DEST RECORDS COUNT", 22),
				leftPadding("COUNTS MATCH", 15), leftPadding("TOTAL PROCESSING TIME", 40) }, false);
		sb = createTable(sb, new Object[] { "TABLE/TITLE", "START TIME", "END TIME", "SOURCE RECORDS COUNT",
				"DEST RECORDS COUNT", "COUNTS MATCH", "TOTAL PROCESSING TIME" }, true, true, false, false);
		blobinfo = createTable(blobinfo, new Object[] { " TABLE", " COLUMN", " SRC BLOBS COUNT", " DEST BLOBS COUNT" },
				true, false, false, true);
		error = createTable(error, new Object[] { " TABLE/TITLE", " ERROR" }, true, false, true, false);

		System.out.println("Extraction Info");
		viewFormatter(prepareObjectArray(7), true);
		for (String stat : statistics.keySet()) {
			sb = createData(sb,
					new Object[] { stat, statistics.get(stat).getStartTime().toGMTString(),
							statistics.get(stat).getEndTime().toGMTString(), statistics.get(stat).getSourceRecords(),
							Integer.toString(statistics.get(stat).getRecordsProcessed()),
							statistics.get(stat).getStatus(), timeDiff(statistics.get(stat).getEndTime().getTime()
									- statistics.get(stat).getStartTime().getTime()) },
					true, false, false);
			viewFormatter(new Object[] { stat, " " + dateFormat.format(statistics.get(stat).getStartTime()),
					" " + dateFormat.format(statistics.get(stat).getEndTime()),
					leftPadding(statistics.get(stat).getSourceRecords(), 22),
					leftPadding(Integer.toString(statistics.get(stat).getRecordsProcessed()), 22),
					leftPadding(statistics.get(stat).getStatus(), 15),
					leftPadding(timeDiff(statistics.get(stat).getEndTime().getTime()
							- statistics.get(stat).getStartTime().getTime()), 40) },
					false);
		}
		viewFormatter(prepareObjectArray(7), true);

		System.out.println();
		System.out.println("Blob Info");
		viewFormatterBlobInfo(prepareObjectArrayBlobInfo(4), true);
		viewFormatterBlobInfo(new Object[] { " TABLE", " COLUMN", " SRC BLOBS COUNT", " DEST BLOBS COUNT" }, false);
		viewFormatterBlobInfo(prepareObjectArrayBlobInfo(4), true);

		for (String stat : statistics.keySet()) {
			TreeMap<String, Long> srcCounts = statistics.get(stat).getSrcBlobCount();
			TreeMap<String, Long> destCounts = statistics.get(stat).getDestBlobCount();
			if (srcCounts.size() > 0) {
				writeblobinfo = true;
				for (String column : srcCounts.keySet()) {
					blobinfo = createData(blobinfo,
							new Object[] { stat, column, srcCounts.get(column), destCounts.get(column) }, false, false,
							true);
					viewFormatterBlobInfo(new Object[] { stat, column, srcCounts.get(column), destCounts.get(column) },
							false);
				}
			}
		}
		if (!writeblobinfo) {
			System.out.println("No Blobs available");
		}
		viewFormatterBlobInfo(prepareObjectArrayBlobInfo(4), true);

		System.out.println();
		System.out.println("Error Info");

		viewFormatterError(prepareObjectArrayError(2), true);
		viewFormatterError(new Object[] { " TABLE", " ERROR" }, false);
		viewFormatterError(prepareObjectArrayError(2), true);
		for (String stat : statistics.keySet()) {
			String errormessage = statistics.get(stat).getError();
			if (errormessage != null && !errormessage.isEmpty()) {
				writeerror = true;
				error = createData(error, new Object[] { stat, errormessage }, false, true, false);
				viewFormatterError(new Object[] { stat, errormessage }, false);
			}
		}
		if (!writeerror) {
			System.out.println("no errors");
		}
		viewFormatterError(prepareObjectArrayError(2), true);

		sb = createTable(sb, null, false, true, false, false);
		blobinfo = writeblobinfo ? createTable(blobinfo, null, false, false, false, true) : new StringBuffer();
		error = writeerror ? createTable(error, null, false, false, true, false) : new StringBuffer();

		if (inputArgs.generateSummaryReportPath != null) {
			generateSummaryReport(sb, error, inputArgs.generateSummaryReportPath, inputArgs.reportId);
		}
		sb = createHeader(sb, inputArgs, queryBuffer.toString());
		sb = createFooter(blobinfo, error, sb, new Object[] { timeDiff(new Date().getTime() - startTime.getTime()) },
				inputArgs.reportId);
		if (!inputArgs.outputFormat.equals("arxml"))
			createReport(sb, inputArgs.outputPath, inputArgs.reportId);

		if (writeerror) {
			throw new Exception("Error occured during extraction. Please refer Extraction Report generated at "
					+ inputArgs.outputPath + " for details");
		} else if (inputArgs.manualMode)
			FileUtil.deleteFile(extractionTableInfoCsvFile.getAbsolutePath());
	}

	boolean start = true;

	@SuppressWarnings("resource")
	private void doJob(String inputArgsqueryTitle, String inputArgsquery, SchemaCrawlerOptions queryDumpOptions,
			boolean version, String blobColumn, StringBuffer queryBuffer) throws Exception {

		String str = inputArgsqueryTitle;
		String[] result = new String[2];

		char[] arr = str.toCharArray();

//		switch (inputArgs.getDatabaseServer().toLowerCase()) {
//		case ServerConstants.MYSQL_LOWER:
//
//			result = getWithTilde(arr);
//			break;
//
//		default:
		result = getWithBrackets(arr);
//			break;
//		}
		String schema = result[0];
		String tableName = result[1];

		System.gc();
		System.out.println(rightPadding(inputArgsqueryTitle, 25) + " -- : " + "Processing Query ");
		Statistics st = new Statistics();
		try {
			if (start) {
				startTime = new Date();
				start = false;
			}
			st.setStartTime(new Date());
		} catch (Exception e) {
			e.printStackTrace();
		}
		String query = null;
		OperationExecutable opterationExe = new OperationExecutable(tableName);
		OperationOptions operationOptions = new OperationOptions();
		Config c = new Config();

		c.put(tableName, inputArgsquery);
		query = inputArgsquery;
		queryBuffer.append(tableName).append(":QUERY:").append(query).append("\n");
		opterationExe.setAdditionalConfiguration(c);
		operationOptions.setShowUnqualifiedNames(true);
		operationOptions.setVersion4(version);
		operationOptions.setSchema(schema);
		operationOptions.setXmlChunkLimit(Constants.MAX_RECORD_PER_XML_FILE);
		operationOptions.setShowLobs(Constants.SHOW_LOB);
		operationOptions.setDateWithDateTime(Constants.SHOW_DATETIME);
		operationOptions.setSipExtract(Constants.IS_SIP_EXTRACT);
		opterationExe.setSchemaCrawlerOptions(queryDumpOptions);

		OutputOptions outputOptions = new OutputOptions();
		outputOptions.setSplitDate(Constants.SPLITDATE);
		outputOptions.setReplacementMap(inputArgs.getReplacementMap());
		outputOptions.setOutputFormatValue(inputArgs.outputFormat);
		Path path = null;

		String outputPath = "Output";
		if (inputArgs.outputFormat.equalsIgnoreCase("file")) {
			operationOptions.setShowLobs(true);
			operationOptions.setFilename(true);
			operationOptions.setFilenameColumn("queryfile");
			operationOptions.setSeqStartValue(1);

			if (inputArgs.outputPath != null && !inputArgs.outputPath.equalsIgnoreCase(""))
				outputPath = inputArgs.outputPath;

			new File(outputPath).mkdirs();
			new File(outputPath + File.separator + "tables").mkdirs();
			new File(outputPath + File.separator + "BLOBs").mkdirs();

			new File(outputPath + File.separator + "BLOBs" + File.separator
					+ Validations.checkValidFolder(tableName.toUpperCase())).mkdirs();
			new File(outputPath + File.separator + "BLOBs" + File.separator
					+ Validations.checkValidFolder(tableName.toUpperCase()) + File.separator
					+ Validations.checkValidFolder(blobColumn.toUpperCase())).mkdirs();
			path = Paths.get(outputPath + File.separator + "TEMP.xml");
			outputOptions.setOutputFile(path);

			if (inputArgs.getBlobIdentifier() != null)
				operationOptions.setBlobFolderidentifier(inputArgs.getBlobIdentifier());

			outputOptions.setOutputFile(path);
			operationOptions.setAppendOutput(false);
			operationOptions.setSchema(schema);

			operationOptions.setBlobTableName(getTextFormatted(tableName.toUpperCase()));
			operationOptions.setBlobColumnName(getTextFormatted(blobColumn.toUpperCase()));
			opterationExe.setOutputOptions(outputOptions);
			opterationExe.setOperationOptions(operationOptions);

			final Catalog udeCatalog = SchemaCrawlerUtility.getCatalog(connection, queryDumpOptions);
			System.out.println("Executing query mode unstructed Date Extraction Job");
			try {
				opterationExe.executeOn(udeCatalog, connection);
				st.setRecordsProcessed(outputOptions.getRecordsProcessed());
				st.setSrcBlobCount(outputOptions.getSrcColumnBlobCount());
				st.setDestBlobCount(outputOptions.getDestColumnBlobCount());
			} catch (Exception e) {
				st.setSourceRecords("0");
				st.setEndTime(new Date());
				st.setStatus("Error");
				st.setError(e.getMessage());
				statistics.put(inputArgsqueryTitle, st);
				if (inputArgs.outputFormat.endsWith("xml") || inputArgs.outputFormat.equalsIgnoreCase("file"))
					path.toFile().delete();
				System.gc();
				return;
			}
		} else {
			if (!inputArgs.outputFormat.equalsIgnoreCase("console")) {
				if (inputArgs.outputPath != null && !inputArgs.outputPath.equalsIgnoreCase(""))
					outputPath = inputArgs.outputPath;
				new File(outputPath + File.separator + (inputArgs.outputFormat.equalsIgnoreCase("arxml") ? "Data" : ""))
						.mkdirs();

				path = Paths.get(outputPath + File.separator
						+ (inputArgs.outputFormat.equalsIgnoreCase("arxml") ? "Data" + File.separator : "")
						+ (version ? Validations.checkValidFile(schema.toUpperCase()) + "-" : "")
						+ Validations.checkValidFile(tableName.toUpperCase()) + "."
						+ inputArgs.outputFormat.toLowerCase());

				if (inputArgs.outputFormat.equalsIgnoreCase("arxml")) {
					Path arpath = Paths.get(outputPath + File.separator + "Analysis_Report.html");
					outputOptions.setArReport(arpath);
				}

				outputOptions.setOutputFile(path);
			}
			if (Constants.MOVE_FILE_TO_NAS) {
				outputOptions.setNasFilePath(Constants.NAS_FILE_PATH);
				outputOptions.setMoveFileToNas(true);
			}
			outputOptions.setBlobRefAttribute(inputArgs.getBlobRefTag());

			opterationExe.setOutputOptions(outputOptions);
			opterationExe.setOperationOptions(operationOptions);
			final Catalog tableDumpCatalog = SchemaCrawlerUtility.getCatalog(connection, queryDumpOptions);
			try {
				opterationExe.executeOn(tableDumpCatalog, connection);
				st.setRecordsProcessed(outputOptions.getRecordsProcessed());
				st.setSrcBlobCount(outputOptions.getSrcColumnBlobCount());
				st.setDestBlobCount(outputOptions.getDestColumnBlobCount());
			} catch (Exception e) {
				st.setSourceRecords("0");
				st.setEndTime(new Date());
				st.setStatus("Error");
				st.setError(e.getMessage());
				statistics.put(inputArgsqueryTitle, st);
				if (inputArgs.outputFormat.endsWith("xml") || inputArgs.outputFormat.equalsIgnoreCase("file"))
					path.toFile().delete();
				System.gc();
				return;
			}
		}

		if (inputArgs.outputFormat.endsWith("xml") || inputArgs.outputFormat.equalsIgnoreCase("file"))
			path.toFile().delete();
		else {
			if (Constants.MOVE_FILE_TO_NAS)
				FileUtil.movefile(path.toFile().getAbsolutePath(), Constants.NAS_FILE_PATH, false);
		}

		String q = query;
		if (query.toLowerCase().contains("order by")) {
			q = q.substring(0, q.toLowerCase().indexOf(" order by"));
		}
		String queryFormulation = "select count(1) " + q.substring(q.toLowerCase().indexOf(" from"));
		st.setSourceRecords(opterationExe.executeCountOn(queryFormulation, connection));
		st.setEndTime(new Date());
		st.setStatus(Integer.toString(st.getRecordsProcessed()).equals(st.getSourceRecords()) ? "True" : "False");

		if (Constants.IS_SIP_EXTRACT) {
			try {
				SipPackager sipPackager = new SipPackager(schema, tableName, inputArgs.outputPath, inputArgs, version,
						outputOptions.getDatatype());
				sipPackager.generateSip();
			} catch (Exception e) {
				e.printStackTrace();
				st.setError("SIP creation error. Reason : " + e.getMessage());
			}
		}

		statistics.put(inputArgsqueryTitle, st);
		System.gc();
		if (inputArgs.isManualMode()) {
			try {
				String countMatch = Integer.toString(st.getRecordsProcessed()).equals(st.getSourceRecords()) ? "True"
						: "False";
				FileUtil.writeFileAppend(extractionTableInfoCsvFile.getAbsolutePath(),
						escapeAndQuoteCsv(countMatch) + ","
								+ escapeAndQuoteCsv(String.valueOf(st.getRecordsProcessed())) + ","
								+ escapeAndQuoteCsv(String.valueOf(st.getEndTime().getTime())) + ","
								+ escapeAndQuoteCsv(String.valueOf(st.getSourceRecords())) + ","
								+ escapeAndQuoteCsv(String.valueOf(st.getStartTime().getTime())) + ","
								+ escapeAndQuoteCsv(inputArgsqueryTitle) + "," + escapeAndQuoteCsv(query) + ","
								+ escapeAndQuoteCsv(computeBlobInfo(st)) + "\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {

			ExtractionStatusDAO esd = new ExtractionStatusDAO();
			ExtractionStatus es = new ExtractionStatus();
			es.setCountMatch(
					Integer.toString(st.getRecordsProcessed()).equals(st.getSourceRecords()) ? "True" : "False");
			es.setDestRecord(st.getRecordsProcessed());
			es.setEndTime(st.getEndTime());
			es.setJobAttempt(inputArgs.getRowId().getJobAttempt());
			es.setJobId(inputArgs.getRowId().getJobId());
			es.setRunId(inputArgs.getRowId().getRunId());
			es.setSourceRecord(st.getSourceRecords());
			es.setStartTime(st.getStartTime());
			es.setTableName(inputArgsqueryTitle);
			es.setBlobinfo(computeBlobInfo(st));
			es.setQuery(query);
			esd.addRecord(es);
		}
	}

	public static String[] getWithTilde(char[] arr) {
		String[] result = new String[2];

		String res = "";
		int i = 0;
		boolean openFlag = false;
		for (char c : arr) {

			if (c == '`') {
				openFlag = !openFlag;
				if (!openFlag) {
					result[i++] = res;
					res = "";
				}

			}

			else if (openFlag)
				res += c;

		}
		return result;
	}

	public static String[] getWithBrackets(char[] arr) {
		String[] result = new String[2];

		String res = "";
		int i = 0;
		boolean openFlag = false, closeFlag = false;
		for (char c : arr) {

			if (c == '[') {
				if (openFlag)
					res += c;
				openFlag = true;
				closeFlag = false;
				continue;
			}
			if (c == ']') {
				closeFlag = true;
				openFlag = false;
				result[i++] = res;
				res = "";
			}

			if (openFlag && !closeFlag)
				res += c;
		}
		return result;
	}

	private String computeBlobInfo(Statistics st) {
		StringBuffer sb = new StringBuffer();
		for (String key : st.getSrcBlobCount().keySet()) {
			sb.append(SPLIT).append(key).append(SEPERATOR).append(st.getSrcBlobCount().get(key)).append(SEPERATOR)
					.append(st.getDestBlobCount().get(key));
		}
		return sb.length() > 0 ? sb.toString().substring(1) : null;
	}

	public static String getTextFormatted(String string) {
		string = string.trim().replace("$", "").replace("(", "").replace(")", "").replace("[", "").replace("]", "")
				.replace("{", "").replace("}", "").replace("/", "").replace("\\", "").replace("\"", "").replace("'", "")
				.replace("`", "").replace("&", "").replace("@", "").replace("#", "").replace("%", "").replace("!", "")
				.replace("^", "").replace("*", "").replace("|", "").replaceAll("\\s+", "_");
		return (string.charAt(0) >= '0' && string.charAt(0) <= '9') ? "_" + string : string;
	}
}
