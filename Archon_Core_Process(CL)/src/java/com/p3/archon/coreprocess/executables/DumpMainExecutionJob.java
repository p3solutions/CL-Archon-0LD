package com.p3.archon.coreprocess.executables;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
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
import com.p3.archon.constants.ServerConstants;
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
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaInfoLevelBuilder;
import schemacrawler.tools.analysis.counts.CatalogWithCounts;
import schemacrawler.tools.analysis.counts.CountsUtility;
import schemacrawler.utility.SchemaCrawlerUtility;

public class DumpMainExecutionJob extends ExecutionJob implements JobExecutable {

	int maxThread = Constants.MAX_TABLES_PARALLEL_EXT;
	int counter = 0;

	Date startTime = new Date();

	boolean start = true;

	Map<String, Statistics> statistics = new LinkedHashMap<String, Statistics>();

	List<String> extractedTables = new ArrayList<>();
	List<ExtractionStatus> extractionStatus;
	File extractionTableInfoCsvFile;

	public DumpMainExecutionJob(DataSource dataSource, Connection connection, ArchonInputBean inputArgs) {
		super(dataSource, connection, inputArgs);
	}

	@SuppressWarnings("deprecation")
	@Override
	public void start() throws Exception {

		printLines(new String[] { "Data Dump", "---------", "" });
		final SchemaCrawlerOptions tableDumpOptions = schemaCrawlerOptionsBuilder(inputArgs, false,
				SchemaInfoLevelBuilder.minimum(), false, false, false);

		Catalog xcatalog = SchemaCrawlerUtility.getCatalog(connection, tableDumpOptions);
		final Catalog catalog = new CatalogWithCounts(xcatalog, getConnection(), tableDumpOptions);
		boolean version = inputArgs.getVersion().trim().equals("4");
		Statistics st;
		if (inputArgs.isManualMode()) {
			extractionTableInfoCsvFile = new File(inputArgs.outputPath + File.separator + "ExtractedTable.csv");
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
				}
				csvReader.close();
				filereader.close();
			}
		} else {
			ExtractionStatusDAO esd = new ExtractionStatusDAO();
			boolean result = esd.isRecordExists(inputArgs.getRowId());
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

		for (final Table table : catalog.getTables()) {
			if (!extractedTables.contains(table.getName())) {
				while (counter == maxThread)
					Thread.sleep(200);
				Thread t = new Thread(runner(table, version, tableDumpOptions, catalog));
				counter++;
				t.start();
				Thread.sleep(500);

			} else
				System.out
						.println(rightPadding(table.getName(), 25) + " -- : " + "Table already Processed. Skipping...");
		}

		while (counter != 0)
			Thread.sleep(200);

		printLines(new String[] { "", "", "Statistics", "----------", "",
				"Tot Extraction Time: " + timeDiff(new Date().getTime() - startTime.getTime()), "" });

		StringBuffer sb = new StringBuffer();
		StringBuffer error = new StringBuffer();
		StringBuffer blobinfo = new StringBuffer();
		boolean writeerror = false;
		boolean writeblobinfo = false;
		viewFormatter(prepareObjectArray(7), true);
		viewFormatter(new Object[] { " TABLE", " START TIME", " END TIME", leftPadding("SOURCE RECORDS COUNT", 22),
				leftPadding("DEST RECORDS COUNT", 22), leftPadding("COUNTS MATCH", 15),
				leftPadding("TOTAL PROCESSING TIME", 40) }, false);
		sb = createTable(sb, new Object[] { "TABLE", "START TIME", "END TIME", "SOURCE RECORDS COUNT",
				"DEST RECORDS COUNT", "COUNTS MATCH", "TOTAL PROCESSING TIME" }, true, true, false, false);
		blobinfo = createTable(blobinfo, new Object[] { " TABLE", " COLUMN", " SRC BLOBS COUNT", " DEST BLOBS COUNT" },
				true, false, false, true);
		error = createTable(error, new Object[] { " TABLE", " ERROR" }, true, false, true, false);

		System.out.println("Extraction Info");
		viewFormatter(prepareObjectArray(7), true);
		for (String stat : statistics.keySet()) {
			sb = createData(sb,
					new Object[] { Validations.checkValidFile(stat), statistics.get(stat).getStartTime().toGMTString(),
							statistics.get(stat).getEndTime().toGMTString(), statistics.get(stat).getSourceRecords(),
							Integer.toString(statistics.get(stat).getRecordsProcessed()),
							statistics.get(stat).getStatus(), timeDiff(statistics.get(stat).getEndTime().getTime()
									- statistics.get(stat).getStartTime().getTime()) },
					true, false, false);
			viewFormatter(new Object[] { Validations.checkValidFile(stat),
					" " + Constants.dateFormat.format(statistics.get(stat).getStartTime()),
					" " + Constants.dateFormat.format(statistics.get(stat).getEndTime()),
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

		if (inputArgs.generateSummaryReportPath != null)
			generateSummaryReport(sb, error, inputArgs.generateSummaryReportPath, inputArgs.reportId);
		sb = createHeader(sb, inputArgs, null);
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

	private Runnable runner(Table table, boolean version, SchemaCrawlerOptions tableDumpOptions, Catalog catalog) {
		return new Runnable() {

			@Override
			public void run() {
				System.gc();
				ArchonInputBean ia = new ArchonInputBean();
				Statistics st = new Statistics();
				ia.command = inputArgs.command;
				ia.databaseName = inputArgs.databaseName;
				ia.databaseServer = inputArgs.databaseServer;
				ia.hostName = inputArgs.hostName;
				ia.includeRelationship = inputArgs.includeRelationship;
				ia.outputFormat = inputArgs.outputFormat;
				ia.outputPath = inputArgs.outputPath;
				ia.password = inputArgs.password;
				ia.portNumber = inputArgs.portNumber;
				ia.queryText = inputArgs.queryText;
				ia.queryFile = inputArgs.queryFile;
				ia.queryTitle = inputArgs.queryTitle;
				ia.queryType = inputArgs.queryType;

				switch (inputArgs.getDatabaseServer().toLowerCase()) {
				case ServerConstants.SQL_LOWER:
				case ServerConstants.SQLWINAUTH_LOWER:
				case ServerConstants.SYBASE_LOWER:
					ia.schemaName = table.getSchema().getName();
					break;

				default:
					ia.schemaName = table.getSchema().getCatalogName();
					break;
				}

				ia.tableInclusionRule = inputArgs.tableInclusionRule;
				ia.userName = inputArgs.userName;
				ia.version = inputArgs.version;
				ia.replacementMap = inputArgs.getReplacementMap();
				ia.applicationName = inputArgs.applicationName;
				ia.holdingPrefix = inputArgs.holdingPrefix;
				ia.blobReferenceAttribute = inputArgs.getBlobRefTag();
				ia.setTable(table.getName());
				try {
					if (start) {
						startTime = new Date();
						start = false;
					}
					st.setStartTime(new Date());
					new DumpExecutionJob(st, dataSource, connection, ia, table, version, tableDumpOptions, catalog)
							.start();
				} catch (Exception e) {
					e.printStackTrace();
					st.setError(e.getMessage());
					st.setSourceRecords(countformatter(CountsUtility.getRowCountMessage(table)));
					st.setEndTime(new Date());
					st.setStatus("Error");
					statistics.put(table.getName(), st);
					counter--;
					System.gc();
					return;
				}
				st.setSourceRecords(countformatter(CountsUtility.getRowCountMessage(table)));
				st.setEndTime(new Date());
				st.setStatus(
						Integer.toString(st.getRecordsProcessed()).equals(st.getSourceRecords()) ? "True" : "False");
				statistics.put(table.getName(), st);
				counter--;
				System.gc();

				if (inputArgs.isManualMode()) {
					try {
						String countMatch = Integer.toString(st.getRecordsProcessed()).equals(st.getSourceRecords())
								? "True"
								: "False";
						FileUtil.writeFileAppend(extractionTableInfoCsvFile.getAbsolutePath(),
								escapeAndQuoteCsv(countMatch) + ","
										+ escapeAndQuoteCsv(String.valueOf(st.getRecordsProcessed())) + ","
										+ escapeAndQuoteCsv(String.valueOf(st.getEndTime().getTime())) + ","
										+ escapeAndQuoteCsv(String.valueOf(st.getSourceRecords())) + ","
										+ escapeAndQuoteCsv(String.valueOf(st.getStartTime().getTime())) + ","
										+ escapeAndQuoteCsv(table.getName()) + "," + escapeAndQuoteCsv("null") + ","
										+ escapeAndQuoteCsv(computeBlobInfo(st)) + "\n");
					} catch (IOException e) {
						e.printStackTrace();
					}
				} else {

					ExtractionStatusDAO esd = new ExtractionStatusDAO();
					ExtractionStatus es = new ExtractionStatus();
					es.setCountMatch(Integer.toString(st.getRecordsProcessed()).equals(st.getSourceRecords()) ? "True"
							: "False");
					es.setDestRecord(st.getRecordsProcessed());
					es.setEndTime(st.getEndTime());
					es.setJobAttempt(inputArgs.getRowId().getJobAttempt());
					es.setJobId(inputArgs.getRowId().getJobId());
					es.setRunId(inputArgs.getRowId().getRunId());
					es.setSourceRecord(st.getSourceRecords());
					es.setStartTime(st.getStartTime());
					es.setTableName(table.getName());
					es.setBlobinfo(computeBlobInfo(st));
					esd.addRecord(es);
				}

			}

			private String computeBlobInfo(Statistics st) {
				StringBuffer sb = new StringBuffer();
				for (String key : st.getSrcBlobCount().keySet()) {
					sb.append(SPLIT).append(key).append(SEPERATOR).append(st.getSrcBlobCount().get(key))
							.append(SEPERATOR).append(st.getDestBlobCount().get(key));
				}
				return sb.length() > 0 ? sb.toString().substring(1) : null;
			}

			private String countformatter(String rowCountMessage) {
				if (rowCountMessage == null)
					return "0";
				else if (rowCountMessage.trim().equals("empty"))
					return "0";
				else
					return rowCountMessage.replace("rows", "").replace(",", "").trim();
			}
		};
	}
}