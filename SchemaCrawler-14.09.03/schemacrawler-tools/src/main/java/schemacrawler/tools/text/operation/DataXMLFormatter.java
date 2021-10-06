/*
========================================================================
SchemaCrawler
http://www.schemacrawler.com
Copyright (c) 2000-2016, Sualeh Fatehi <sualeh@hotmail.com>.
All rights reserved.
------------------------------------------------------------------------

SchemaCrawler is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

SchemaCrawler and the accompanying materials are made available under
the terms of the Eclipse Public License v1.0, GNU General Public License
v3 or GNU Lesser General Public License v3.

You may elect to redistribute this code under any of these licenses.

The Eclipse Public License is available at:
http://www.eclipse.org/legal/epl-v10.html

The GNU General Public License v3 and the GNU Lesser General Public
License v3 are available at:
http://www.gnu.org/licenses/

========================================================================
*/

package schemacrawler.tools.text.operation;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.UUID;

import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.tools.options.TextOutputFormat;
import schemacrawler.tools.text.base.BaseXMLFormatter;
import schemacrawler.tools.text.operation.blobbean.BlobInfo;
import schemacrawler.tools.text.utility.BinaryData;
import schemacrawler.tools.text.utility.FileUtil;
import schemacrawler.tools.text.utility.XMLFormattingHelper;
import schemacrawler.tools.traversal.DataTraversalHandler;
import schemacrawler.utility.Query;

/**
 * Text formatting of data.
 *
 * @author Malik
 */
final class DataXMLFormatter extends BaseXMLFormatter<OperationOptions> implements DataTraversalHandler {

	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S");
	private static final int THRESHOLD = 1 * 1024 * 1024;
	private final Operation operation;

	/**
	 * Text formatting of data.
	 *
	 * @param operation
	 *            Options for text formatting of data
	 * @param options
	 *            Options for text formatting of data
	 * @param outputOptions
	 *            Options for text formatting of data
	 */
	DataXMLFormatter(final Operation operation, final OperationOptions options, final OutputOptions outputOptions)
			throws SchemaCrawlerException {
		super(options, /* printVerboseDatabaseInfo */false, outputOptions);
		this.operation = operation;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see schemacrawler.tools.traversal.DataTraversalHandler#handleData(schemacrawler.utility.Query,
	 *      java.sql.ResultSet)
	 */
	@Override
	public void handleData(final Query query, final ResultSet rows) throws SchemaCrawlerException {
		String title;
		if (query != null) {
			title = query.getName();
		} else {
			title = "";
		}

		handleData(title, rows);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see schemacrawler.tools.traversal.DataTraversalHandler#handleData(schemacrawler.schema.Table,
	 *      java.sql.ResultSet)
	 */
	@Override
	public void handleData(final Table table, final ResultSet rows) throws SchemaCrawlerException {
		final String tableName;
		if (table != null) {
			if (options.isShowUnqualifiedNames()) {
				tableName = table.getName();
			} else {
				tableName = table.getFullName();
			}
		} else {
			tableName = "";
		}
		handleData(tableName, rows);
	}

	/**
	 * Handles an aggregate operation, such as a count, for a given table.
	 *
	 * @param title
	 *            Title
	 * @param results
	 *            Results
	 */
	private long handleAggregateOperationForTable(final String title, final ResultSet results)
			throws SchemaCrawlerException {
		try {
			long aggregate = 0;
			if (results.next()) {
				aggregate = results.getLong(1);
			}
			return aggregate;
		} catch (final SQLException e) {
			throw new SchemaCrawlerException("Could not obtain aggregate data", e);
		}
	}

	private void handleData(final String title, final ResultSet rows) throws SchemaCrawlerException {
		System.out.println("Processing " + title);
		if (rows == null) {
			return;
		}
		if (operation == Operation.count) {
			formattingHelper.writeRecordStart("COUNT", "-ROW");
			formattingHelper.writeElementStart("TABLE_NAME");
			formattingHelper.writeValue(title);
			formattingHelper.writeElementEnd();
			formattingHelper.writeElementStart("COUNT");
			final long aggregate = handleAggregateOperationForTable(title, rows);
			formattingHelper.writeValue(Long.toString(aggregate));
			formattingHelper.writeElementEnd();
			formattingHelper.writeRecordEnd();
		} else {
			try {
				final DataResultSet dataRows = new DataResultSet(rows, options.isShowLobs(),
						options.isDateWithDateTime());
				dataRows.computeColumnInfo();
				outputOptions.setDatatype(dataRows.dataMap);
				iterateRows(dataRows, dataRows.getColumnName(), dataRows.getColumnType(), dataRows.getColumnTypeJava(),
						title, dataRows.getSortedOrderForOracleFix());
			} catch (final Exception e) {
				e.printStackTrace();
				throw new SchemaCrawlerException(e.getMessage(), e);
			}
		}
	}

	private static CharSequence numberFormatter(int i) {
		return String.format("%05d", i);
	}

	@SuppressWarnings("unused")
	private int computeCheckLimit(int length) {
		if (length >= 1000)
			return 500;
		if (length >= 750)
			return 1000;
		if (length >= 500)
			return 2000;
		if (length >= 250)
			return 3000;
		if (length >= 200)
			return 5000;
		if (length >= 100)
			return 10000;
		if (length >= 50)
			return 15000;
		return 20000;
	}

	private void iterateRows(DataResultSet dataRows, String[] columns, String[] columnsType, int[] columnsTypeJava,
			String tablename, List<Integer> oracleFixSort) throws SQLException, IOException {
		Date startDate = new Date();
		System.out.println(rightPadding(tablename, 25) + " -- : " + "\t -> ResultSet gathered. XML writing started at "
				+ dateFormat.format(startDate));

		// int computeCheckLimit = computeCheckLimit(columns.length);
		int recordsProcessed = 0;
		int counter = 0;

		final TextOutputFormat outputFormat = TextOutputFormat.valueOfFromString(outputOptions.getOutputFormatValue());

		if (XMLFormattingHelper.getCharReplace() == null) {
			XMLFormattingHelper.setCharReplace(outputOptions.getReplacementMap());
		}

		final Path path = outputOptions.getOutputFile();
		String newPath = path.toString().replace(".arxml", ".xml").replace(".xml",
				"-" + numberFormatter(counter++) + ".xml");
		Path pathloc = Paths.get(newPath);

		// File f = pathloc.toFile();
		outputOptions.setOutputFile(pathloc);
		out = new PrintWriter(outputOptions.openNewOutputWriter(options.isAppendOutput()), true);
		formattingHelper = ((outputFormat == TextOutputFormat.arxml)
				? new XMLFormattingHelper(out, outputFormat, newPath)
				: new XMLFormattingHelper(out, outputFormat));
		formattingHelper.setInfo(0, tablename);
		formattingHelper.writeDocumentStart();
		if (options.isVersion4())
			formattingHelper.writeRootElementStart(options.getSchema());
		formattingHelper.writeRootElementStart(tablename);
		if (options.isDbLinkPull()) {
			formattingHelper.writeAttribute("SCHEMA", options.getDbLinkSchema());
			formattingHelper.writeAttribute("TABLE", options.getDbLinkTableName());
			formattingHelper.writeAttribute("DBLINK_TABLENAME", options.getDbLinkFullTableName());
		}
		FileChannel f = FileChannel.open(pathloc, StandardOpenOption.READ);
		while (dataRows.next()) {
			formattingHelper.setInfo(recordsProcessed + 1, tablename);
			out.flush();
			// if (recordsProcessed % computeCheckLimit != 0) {
			//
			// } else
			if (f.size() >= (options.getXmlChunkLimit() - THRESHOLD)) {
				formattingHelper.writeRootElementEnd();
				if (options.isVersion4())
					formattingHelper.writeRootElementEnd();
				formattingHelper.writeDocumentEnd();
				out.flush();
				formattingHelper.flush();
				System.out.println(rightPadding(tablename, 25) + " -- : " + "\t --> " + recordsProcessed
						+ " record(s) processed. (Write Time Elapsed : "
						+ timeDiff(new Date().getTime() - startDate.getTime()) + ")");

				if (outputFormat == TextOutputFormat.arxml)
					new File(newPath).delete();

				newPath = path.toString().replace(".arxml", ".xml").replace(".xml",
						"-" + numberFormatter(counter++) + ".xml");
				pathloc = Paths.get(newPath);
				// f = pathloc.toFile();
				if (f != null && f.isOpen())
					f.close();
				if (outputOptions.isMoveFileToNas() && !options.isSipExtract())
					FileUtil.movefile(pathloc.toFile().getAbsolutePath(), outputOptions.getNasFilePath(), false);

				outputOptions.setOutputFile(pathloc);
				out = new PrintWriter(outputOptions.openNewOutputWriter(options.isAppendOutput()), true);
				formattingHelper = ((outputFormat == TextOutputFormat.arxml)
						? new XMLFormattingHelper(out, outputFormat, newPath)
						: new XMLFormattingHelper(out, outputFormat));
				formattingHelper.setInfo(0, tablename);
				formattingHelper.writeDocumentStart();
				if (options.isVersion4())
					formattingHelper.writeRootElementStart(options.getSchema());
				formattingHelper.writeRootElementStart(tablename);
				if (options.isDbLinkPull()) {
					formattingHelper.writeAttribute("SCHEMA", options.getDbLinkSchema());
					formattingHelper.writeAttribute("TABLE", options.getDbLinkTableName());
					formattingHelper.writeAttribute("DBLINK_TABLENAME", options.getDbLinkFullTableName());
				}
				f = FileChannel.open(pathloc, StandardOpenOption.READ);
			}

			final List<Object> currentRow = dataRows.rowXml();
			final Object[] columnData = currentRow.toArray(new Object[currentRow.size()]);
			if (options.isVersion4())
				formattingHelper.writeRecordStart("ROW", "");
			else
				formattingHelper.writeRecordStart(tablename, "-ROW");
			formattingHelper.writeAttribute("REC_ID", Integer.toString(recordsProcessed + 1));

			for (int i = 0; i < columnData.length; i++) {
				// System.out.println(columns[i] + "\t:\t" + columnData[i] +
				// "\t:\t" +
				// columnsType[i]);
				if ((columnData[i] != null && options.isSipExtract()) || !options.isSipExtract()) {
					formattingHelper.writeElementStart((String) columns[i]);
					if (columnData[i] == null)
						formattingHelper.writeAttribute("null", "true");
					if (columnsTypeJava[i] == 0) {
						if (columnsType[i].equalsIgnoreCase("DATE") && options.isDateWithDateTime()) {
							String part1 = columnData[i] == null ? "" : columnData[i].toString().split(" ")[0];
							String part2 = "";
							try {
								part2 = columnData[i] == null ? "" : columnData[i].toString().split(" ")[1];
							} catch (Exception e) {
							}
							if (part2.trim().equals(""))
								part2 = "00:00:00.0";

							try {
								formattingHelper
										.writeValue(columnData[i] == null ? "" : part1 + "T" + part2.substring(0, 8));
							} catch (ArrayIndexOutOfBoundsException e) {
								formattingHelper
										.writeValue(columnData[i] == null ? "" : part1 + "T" + part2.substring(0, 8));
							}

						} else
							formattingHelper.writeValue(columnData[i] == null ? "" : columnData[i] + "");
					} else if (options.isShowLobs() && columnsTypeJava[i] > 0) {
						try {
							if (columnsTypeJava[i] == 1) {
								formattingHelper.writeAttribute("type", "CLOB");
								formattingHelper.writeCData(columnData[i] == null ? null : columnData[i].toString());
							} else if (columnsTypeJava[i] == 2) {
								Blob blob = columnData[i] != null ? ((BinaryData) columnData[i]).getBlob() : null;
								if (blob != null) {
									formattingHelper.writeAttribute("type", "BLOB");
									if (!outputOptions.getSrcColumnBlobCount().containsKey(columns[i])) {
										outputOptions.getSrcColumnBlobCount().put(columns[i], (long) 0);
										outputOptions.getDestColumnBlobCount().put(columns[i], (long) 0);
									}
								} else
									formattingHelper.writeValue(columnData[i] == null ? "" : columnData[i] + "");

								if (columnData[i] != null && blob != null) {
									outputOptions.getSrcColumnBlobCount().put(columns[i],
											outputOptions.getSrcColumnBlobCount().get(columns[i]) + 1);
									BlobInfo blobinfo = getBlobInfo(tablename, columns[i], columnData[i],
											recordsProcessed, path.getParent().toString());
									if (blobinfo.getPath() != null)
										blobinfo.setPath(
												blobinfo.getPath()
														.replace(
																path.getParent().toString()
																		+ (path.getParent().toString().endsWith(
																				File.separator) ? "" : File.separator),
																"")
														.replace("\\", "/"));
									if (!options.isSipExtract())
										formattingHelper.writeAttribute(outputOptions.getBlobRefAttribute(),
												blobinfo.getPath());
									else {
										formattingHelper.writeAttribute("ref", blobinfo.getPath());
									}
									formattingHelper.writeAttribute("size", blobinfo.getSize());
									formattingHelper.writeAttribute("status", blobinfo.getStatus());
									formattingHelper.writeValue(blobinfo.getName());
								} else
									formattingHelper.writeValue("");
							} else
								formattingHelper.writeValue(columnData[i] == null ? "" : columnData[i] + "");
						} catch (NumberFormatException e) {
							formattingHelper.writeValue(columnData[i] == null ? "" : columnData[i] + "");
						}
					}
					formattingHelper.writeElementEnd();

					// System.out.println("DS----" + columnsType[i] + "---" +
					// columns[i]);
					if ((columnsType[i].contains("DATETIME") || columnsType[i].contains("TIMESTAMP"))) {
						if (outputOptions.isSplitDate())
							if (columnData[i] == null) {
								formattingHelper.writeElementStart(columns[i] + "_DT_SPLIT");
								formattingHelper.writeAttribute("createdBy", "DL");
								formattingHelper.writeValue("");
								formattingHelper.writeElementEnd();
								formattingHelper.writeElementStart(columns[i] + "_TM_SPLIT");
								formattingHelper.writeAttribute("createdBy", "DL");
								formattingHelper.writeValue("");
								formattingHelper.writeElementEnd();
								formattingHelper.writeElementStart(columns[i] + "_TM_SPLIT_FORMATTED");
								formattingHelper.writeAttribute("createdBy", "DL");
								formattingHelper.writeValue("");
								formattingHelper.writeElementEnd();
								formattingHelper.writeElementStart(columns[i] + "_DTM_SPLIT");
								formattingHelper.writeAttribute("createdBy", "DL");
								formattingHelper.writeValue("");
								formattingHelper.writeElementEnd();
							} else if (columnData[i] != null && columnData[i].toString().equals("")) {
								formattingHelper.writeElementStart(columns[i] + "_DT_SPLIT");
								formattingHelper.writeAttribute("createdBy", "DL");
								formattingHelper.writeValue("");
								formattingHelper.writeElementEnd();
								formattingHelper.writeElementStart(columns[i] + "_TM_SPLIT");
								formattingHelper.writeAttribute("createdBy", "DL");
								formattingHelper.writeValue("");
								formattingHelper.writeElementEnd();
								formattingHelper.writeElementStart(columns[i] + "_TM_SPLIT_FORMATTED");
								formattingHelper.writeAttribute("createdBy", "DL");
								formattingHelper.writeValue("");
								formattingHelper.writeElementEnd();
								formattingHelper.writeElementStart(columns[i] + "_DTM_SPLIT");
								formattingHelper.writeAttribute("createdBy", "DL");
								formattingHelper.writeValue("");
								formattingHelper.writeElementEnd();
							} else if (columnData[i] != null && !columnData[i].toString().equals("")) {
								String part1 = columnData[i].toString().split(" ")[0];
								String part2 = "";
								try {
									part2 = columnData[i].toString().split(" ")[1];
								} catch (Exception e) {
								}
								if (part2 != null) {
									if (part2.trim().equals(""))
										part2 = "00:00:00.0";
									formattingHelper.writeElementStart(columns[i] + "_DT_SPLIT");
									formattingHelper.writeAttribute("createdBy", "DL");
									formattingHelper.writeValue(part1 == null ? "" : part1 + "");
									formattingHelper.writeElementEnd();
									formattingHelper.writeElementStart(columns[i] + "_TM_SPLIT");
									formattingHelper.writeAttribute("createdBy", "DL");
									formattingHelper.writeValue(part2);
									formattingHelper.writeElementEnd();
									formattingHelper.writeElementStart(columns[i] + "_TM_SPLIT_FORMATTED");
									formattingHelper.writeAttribute("createdBy", "DL");
									try {
										formattingHelper.writeValue(part2.substring(0, 8));
									} catch (ArrayIndexOutOfBoundsException e) {
										formattingHelper.writeValue("00:00:00");
									}
									formattingHelper.writeElementEnd();
									formattingHelper.writeElementStart(columns[i] + "_DTM_SPLIT");
									formattingHelper.writeAttribute("createdBy", "DL");
									try {
										formattingHelper.writeValue(part1 + "T" + part2.substring(0, 8));
									} catch (ArrayIndexOutOfBoundsException e) {
										formattingHelper.writeValue(part1 + "T" + part2.substring(0, 8));
									}
									formattingHelper.writeElementEnd();
								}
							}
					}
				}
			}
			formattingHelper.writeRecordEnd();
			recordsProcessed++;
			if (recordsProcessed % 50000 == 0)
				System.out.println(rightPadding(tablename, 25) + " -- : " + "\t --> " + recordsProcessed
						+ " record(s) processed. (Write Time Elapsed : "
						+ timeDiff(new Date().getTime() - startDate.getTime()) + ")");
		}
		formattingHelper.writeRootElementEnd();
		if (options.isVersion4())
			formattingHelper.writeRootElementEnd();
		formattingHelper.writeDocumentEnd();
		out.flush();
		formattingHelper.flush();

		if (outputFormat == TextOutputFormat.arxml) {
			new File(newPath).delete();
			writeArReport();
		}
		if (f != null && f.isOpen())
			f.close();
		if (outputOptions.isMoveFileToNas() && !options.isSipExtract())
			FileUtil.movefile(pathloc.toFile().getAbsolutePath(), outputOptions.getNasFilePath(), false);
		outputOptions.setRecordsProcessed(recordsProcessed);
		System.out.println(rightPadding(tablename, 25) + " -- : " + "\t Extraction Completed. Totally "
				+ recordsProcessed + " record(s) processed. (Total Write Time : "
				+ timeDiff(new Date().getTime() - startDate.getTime()) + ")");
	}

	private BlobInfo getBlobInfo(String tablename, String columnname, Object blob, int recordsProcessed,
			String outputlocation) {
		BlobInfo blobInfo = new BlobInfo();

		String validfilename = UUID.randomUUID().toString().substring(0, 14) + new Date().getTime();
		String folder = outputlocation + (outputlocation.endsWith(File.separator) ? "" : File.separator) + "BLOBs"
				+ File.separator + checkValidFolder(tablename.toUpperCase()) + File.separator
				+ checkValidFolder(columnname.toUpperCase()) + File.separator + "Folder-"
				+ (((recordsProcessed / 250) * 250) + 1) + "-" + (((recordsProcessed / 250) * 250) + 250)
				+ File.separator;

		new File(folder).mkdirs();

		String file = folder + validfilename;

		try {
			BinaryData data = ((BinaryData) blob);

			// InputStream is = new
			// ByteArrayInputStream(data.toString().getBytes(Charset.forName("UTF-8")));
			// MimeTypeDetector detector = new MimeTypeDetector();
			// String mimetype = detector.detectMimeType(file, is);
			// String type = MimeTypeMapping.mimeTypeToExtension(mimetype);
			// file = file + type;

			InputStream in = data.getBlob().getBinaryStream();
			OutputStream out = new FileOutputStream(file);
			byte[] buff = new byte[1024];
			int len = 0;

			while ((len = in.read(buff)) != -1) {
				out.write(buff, 0, len);
			}

			out.flush();
			out.close();
			in.close();

			if (outputOptions.isMoveFileToNas() && !options.isSipExtract()) {
				String nasFolder = outputOptions.getNasFilePath()
						+ (outputOptions.getNasFilePath().endsWith(File.separator) ? "" : File.separator) + "BLOBs"
						+ File.separator + checkValidFolder(tablename.toUpperCase()) + File.separator
						+ checkValidFolder(columnname.toUpperCase()) + File.separator + "Folder-"
						+ (((recordsProcessed / 250) * 250) + 1) + "-" + (((recordsProcessed / 250) * 250) + 250)
						+ File.separator;

				new File(nasFolder).mkdirs();

				FileUtil.movefile(file, nasFolder, false);

			}

			blobInfo.setPath(file);
			blobInfo.setName(new File(file).getName());
			blobInfo.setStatus("Success");
			try {
				blobInfo.setSize(Math.ceil(((double) (new File(file).length()) / (double) 1024)) + " KB");
			} catch (Exception e) {
				blobInfo.setSize("NA");
			}
			outputOptions.getDestColumnBlobCount().put(columnname,
					outputOptions.getDestColumnBlobCount().get(columnname) + 1);
		} catch (Exception e) {
			blobInfo.setPath("");
			blobInfo.setName(new File(file).getName());
			blobInfo.setStatus("Failure");
			blobInfo.setSize("NA");
			e.printStackTrace();
		}
		return blobInfo;
	}

	public static String checkValidFolder(String name) {
		if (name != null)
			return getFolderFormatted(name);
		return "unnamed";
	}

	public static String checkValidFile(String name) {
		if (name != null)
			return getFileFormatted(name);
		return "unnamed";
	}

	public static String getFolderFormatted(String string) {
		string = string.trim().replaceAll("[^_^\\p{Alnum}.]", "_").replace("^", "_").replaceAll("\\s+", "_")
				.toUpperCase();
		string = ((string.startsWith("_") && string.endsWith("_") && string.length() > 2)
				? string.substring(1).substring(0, string.length() - 2)
				: string);
		return string;
	}

	public static String getFileFormatted(String string) {
		string = string.trim().replaceAll("[^_^\\p{Alnum}.]", "_").replace("^", "_").replaceAll("\\s+", "_");
		string = ((string.startsWith("_") && string.endsWith("_") && string.length() > 2)
				? string.substring(1).substring(0, string.length() - 2)
				: string);
		return string;
	}

	private void writeArReport() throws IOException {
		File f1 = new File(outputOptions.getArReport().toString());
		Writer bwh = new OutputStreamWriter(new FileOutputStream(f1), "UTF-8");

		bwh.write(
				"<HEAD><meta http-equiv='COntent-Type' content='text/html;charset=UTF-8'><TITLE>Ananlysis Report</TITLE>"
						+ "\n<style> @page {  size: 18in 12in; } table { background-color: transparent;}caption {  padding-top: 8px;  padding-bottom: 8px;  color: #777;  text-align: left;}th {  text-align: left;}.table {  width: 100%;  max-width: 100%;  margin-bottom: 20px;}.table > thead > tr > th,.table > tbody > tr > th,.table > tfoot > tr > th,.table > thead > tr > td,.table > tbody > tr > td,.table > tfoot > tr > td {  padding: 8px;  line-height: 1.42857143;  vertical-align: top;  border-top: 1px solid #ddd;}.table > thead > tr > th {  vertical-align: bottom;  border-bottom: 2px solid #ddd;}.table > caption + thead > tr:first-child > th,.table > colgroup + thead > tr:first-child > th,.table > thead:first-child > tr:first-child > th,.table > caption + thead > tr:first-child > td,.table > colgroup + thead > tr:first-child > td,.table > thead:first-child > tr:first-child > td {  border-top: 0;}.table > tbody + tbody {  border-top: 2px solid #ddd;}.table .table {  background-color: #fff;}.table-condensed > thead > tr > th,.table-condensed > tbody > tr > th,.table-condensed > tfoot > tr > th,.table-condensed > thead > tr > td,.table-condensed > tbody > tr > td,.table-condensed > tfoot > tr > td {  padding: 5px;}.table-bordered {  border: 1px solid #ddd;}.table-bordered > thead > tr > th,.table-bordered > tbody > tr > th,.table-bordered > tfoot > tr > th,.table-bordered > thead > tr > td,.table-bordered > tbody > tr > td,.table-bordered > tfoot > tr > td {  border: 1px solid #ddd;}.table-bordered > thead > tr > th,.table-bordered > thead > tr > td {  border-bottom-width: 2px;}.table-striped > tbody > tr:nth-of-type(odd) {  background-color: #f9f9f9;}.table-hover > tbody > tr:hover {  background-color: #f5f5f5;}table col[class*=\"col-\"] {  position: static;  display: table-column;  float: none;}table td[class*=\"col-\"],table th[class*=\"col-\"] {  position: static;  display: table-cell;  float: none;}.table > thead > tr > td.active,.table > tbody > tr > td.active,.table > tfoot > tr > td.active,.table > thead > tr > th.active,.table > tbody > tr > th.active,.table > tfoot > tr > th.active,.table > thead > tr.active > td,.table > tbody > tr.active > td,.table > tfoot > tr.active > td,.table > thead > tr.active > th,.table > tbody > tr.active > th,.table > tfoot > tr.active > th {  background-color: #f5f5f5;}.table-hover > tbody > tr > td.active:hover,.table-hover > tbody > tr > th.active:hover,.table-hover > tbody > tr.active:hover > td,.table-hover > tbody > tr:hover > .active,.table-hover > tbody > tr.active:hover > th {  background-color: #e8e8e8;}.table > thead > tr > td.success,.table > tbody > tr > td.success,.table > tfoot > tr > td.success,.table > thead > tr > th.success,.table > tbody > tr > th.success,.table > tfoot > tr > th.success,.table > thead > tr.success > td,.table > tbody > tr.success > td,.table > tfoot > tr.success > td,.table > thead > tr.success > th,.table > tbody > tr.success > th,.table > tfoot > tr.success > th {  background-color: #dff0d8;}.table-hover > tbody > tr > td.success:hover,.table-hover > tbody > tr > th.success:hover,.table-hover > tbody > tr.success:hover > td,.table-hover > tbody > tr:hover > .success,.table-hover > tbody > tr.success:hover > th {  background-color: #d0e9c6;}.table > thead > tr > td.info,.table > tbody > tr > td.info,.table > tfoot > tr > td.info,.table > thead > tr > th.info,.table > tbody > tr > th.info,.table > tfoot > tr > th.info,.table > thead > tr.info > td,.table > tbody > tr.info > td,.table > tfoot > tr.info > td,.table > thead > tr.info > th,.table > tbody > tr.info > th,.table > tfoot > tr.info > th {  background-color: #d9edf7;}.table-hover > tbody > tr > td.info:hover,.table-hover > tbody > tr > th.info:hover,.table-hover > tbody > tr.info:hover > td,.table-hover > tbody > tr:hover > .info,.table-hover > tbody > tr.info:hover > th {  background-color: #c4e3f3;}.table > thead > tr > td.warning,.table > tbody > tr > td.warning,.table > tfoot > tr > td.warning,.table > thead > tr > th.warning,.table > tbody > tr > th.warning,.table > tfoot > tr > th.warning,.table > thead > tr.warning > td,.table > tbody > tr.warning > td,.table > tfoot > tr.warning > td,.table > thead > tr.warning > th,.table > tbody > tr.warning > th,.table > tfoot > tr.warning > th {  background-color: #fcf8e3;}.table-hover > tbody > tr > td.warning:hover,.table-hover > tbody > tr > th.warning:hover,.table-hover > tbody > tr.warning:hover > td,.table-hover > tbody > tr:hover > .warning,.table-hover > tbody > tr.warning:hover > th {  background-color: #faf2cc;}.table > thead > tr > td.danger,.table > tbody > tr > td.danger,.table > tfoot > tr > td.danger,.table > thead > tr > th.danger,.table > tbody > tr > th.danger,.table > tfoot > tr > th.danger,.table > thead > tr.danger > td,.table > tbody > tr.danger > td,.table > tfoot > tr.danger > td,.table > thead > tr.danger > th,.table > tbody > tr.danger > th,.table > tfoot > tr.danger > th {  background-color: #f2dede;}.table-hover > tbody > tr > td.danger:hover,.table-hover > tbody > tr > th.danger:hover,.table-hover > tbody > tr.danger:hover > td,.table-hover > tbody > tr:hover > .danger,.table-hover > tbody > tr.danger:hover > th {  background-color: #ebcccc;}.table-responsive {  min-height: .01%;  overflow-x: auto;}@media screen and (max-width: 767px) {  .table-responsive {    width: 100%;    margin-bottom: 15px;    overflow-y: hidden;    -ms-overflow-style: -ms-autohiding-scrollbar;    border: 1px solid #ddd;  }  .table-responsive > .table {    margin-bottom: 0;  }  .table-responsive > .table > thead > tr > th,  .table-responsive > .table > tbody > tr > th,  .table-responsive > .table > tfoot > tr > th,  .table-responsive > .table > thead > tr > td,  .table-responsive > .table > tbody > tr > td,  .table-responsive > .table > tfoot > tr > td {    white-space: nowrap;  }  .table-responsive > .table-bordered {    border: 0;  }  .table-responsive > .table-bordered > thead > tr > th:first-child,  .table-responsive > .table-bordered > tbody > tr > th:first-child,  .table-responsive > .table-bordered > tfoot > tr > th:first-child,  .table-responsive > .table-bordered > thead > tr > td:first-child,  .table-responsive > .table-bordered > tbody > tr > td:first-child,  .table-responsive > .table-bordered > tfoot > tr > td:first-child {    border-left: 0;  }  .table-responsive > .table-bordered > thead > tr > th:last-child,  .table-responsive > .table-bordered > tbody > tr > th:last-child,  .table-responsive > .table-bordered > tfoot > tr > th:last-child,  .table-responsive > .table-bordered > thead > tr > td:last-child,  .table-responsive > .table-bordered > tbody > tr > td:last-child,  .table-responsive > .table-bordered > tfoot > tr > td:last-child {    border-right: 0;  }  .table-responsive > .table-bordered > tbody > tr:last-child > th,  .table-responsive > .table-bordered > tfoot > tr:last-child > th,  .table-responsive > .table-bordered > tbody > tr:last-child > td,  .table-responsive > .table-bordered > tfoot > tr:last-child > td {    border-bottom: 0; font-size:10px }}</style></HEAD><BODY>");
		bwh.write(
				"<center><b><font style=\"size:10px\">Archon Unsuppored characters Analysis Report</font></b></center><br></br>");

		if (outputOptions.getReplacementMap().size() > 0) {
			bwh.write(
					"<p><font style=\"size:10px\">Replacement Characters added before analysis : </font></p> <center>");
			bwh.write("<TABLE class=\"table table-bordered table-striped\">");
			bwh.write("<TR>");
			bwh.write("<TD width='33%'>");
			bwh.write("<B><Center>Code Point</Center></B>");
			bwh.write("</TD>");
			bwh.write("<TD width='33%'>");
			bwh.write("<B><Center>Hex Representation</Center></B>");
			bwh.write("</TD>");
			bwh.write("<TD width='33%'>");
			bwh.write("<B><Center>Replacement Character</Center></B>");
			bwh.write("</TD>");
			bwh.write("</TR>");
			for (Entry<String, String> r : outputOptions.getReplacementMap().entrySet()) {
				bwh.write("<TR>");
				bwh.write("<TD width='33%'>");
				bwh.write("<Center>" + r.getKey() + "" + "</Center>");
				bwh.write("</TD>");
				bwh.write("<TD width='33%'>");
				try {
					bwh.write("<Center>" + String.format("0x%04X", Integer.parseInt(r.getKey())) + "" + "</Center>");
				} catch (NumberFormatException e) {
					bwh.write("<Center>" + "NA" + "" + "</Center>");
				}
				bwh.write("</TD>");
				bwh.write("<TD width='33%'>");
				bwh.write("<Center>" + r.getValue() + "" + "</Center>");
				bwh.write("</TD>");
				bwh.write("</TR>");
			}
			bwh.write("</TABLE>");
			bwh.write("</center>");
			bwh.write("<hr/>");
			bwh.write("<hr/>");
		}

		if (XMLFormattingHelper.getArMap() == null || XMLFormattingHelper.getArMap().size() == 0) {
			bwh.write("<p>All Data is XML compatible</p>");
		} else {
			bwh.write("<p><font style=\"size:10px\">Analysis Report : </font></p> <center>");
			bwh.write("<TABLE class=\"table table-bordered table-striped\">");
			bwh.write("<TR>");
			bwh.write("<TD width='20%'>");
			bwh.write("<B><Center>Code Point</Center></B>");
			bwh.write("</TD>");
			bwh.write("<TD width='20%'>");
			bwh.write("<B><Center>Hex Representation</Center></B>");
			bwh.write("</TD>");
			bwh.write("<TD width='20%'>");
			bwh.write("<B><Center>Character</Center></B>");
			bwh.write("</TD>");
			bwh.write("<TD width='20%'>");
			bwh.write("<B><Center>Total Occrance</Center></B>");
			bwh.write("</TD>");
			bwh.write("<TD width='20%'>");
			bwh.write("<B><Center>Action</Center></B>");
			bwh.write("</TD>");
			bwh.write("</TR></TABLE>");

			for (int cp : XMLFormattingHelper.getArMap().keySet()) {
				bwh.write("<TABLE class=\"table table-bordered table-striped\">");
				bwh.write("<TR>");
				bwh.write("<TD width='20%'>");
				bwh.write("<center> " + cp + "</center>");
				bwh.write("</TD>");

				bwh.write("<TD width='20%'>");
				bwh.write("<center> " + String.format("0x%04X", cp) + "</center> ");
				bwh.write("</TD>");

				bwh.write("<TD width='20%'>");
				bwh.write("<center> " + Character.toChars(cp)[0] + "</center> ");
				bwh.write("</TD>");

				bwh.write("<TD width='20%'>");
				bwh.write("<center> " + getTotal(cp) + "</center> ");
				bwh.write("</TD>");

				bwh.write("<TD width='20%'><center>");
				bwh.write("<Button onclick=\"toogleView('_" + cp + "')\">Show/Hide</Button>");
				bwh.write("</center></TD>");
				bwh.write("</TR>");
				bwh.write("</TABLE>");

				bwh.write("<div style='display:none;' id='_" + cp
						+ "'><TABLE width='100%' class='table table-bordered table-striped'>");
				bwh.write("<TR>");
				bwh.write("<TH>");
				bwh.write("<B>Table</B>");
				bwh.write("</TH>");
				bwh.write("<TH>");
				bwh.write("<B>No of Occurance</B>");
				bwh.write("</TH>");
				bwh.write("<TH>");
				bwh.write("<B>Occuring Record Ids</B>");
				bwh.write("</TH>");
				bwh.write("</TR>");

				for (Entry<String, TreeSet<Integer>> obj : XMLFormattingHelper.getArMap().get(cp).entrySet()) {
					bwh.write("<TR>");
					bwh.write("<TD>");
					bwh.write(obj.getKey());
					bwh.write("</TD>");
					bwh.write("<TD>");
					bwh.write(obj.getValue().size() + "");
					bwh.write("</TD>");
					bwh.write("<TD>");
					bwh.write("" + obj.getValue());
					bwh.write("</TD>");
					bwh.write("</TR>");
				}
				bwh.write("</TABLE><nr/></div>");
			}
		}
		bwh.write("\n<script>" + "\n	function toogleView(i){" + "\n		var itab = document.getElementById(i);"
				+ "\n		if(itab.style.display == \"block\") " + "\n			itab.style.display = 'none';"
				+ "\n		else " + "\n			itab.style.display = 'block';" + "\n}"
				+ "\n</script></BODY></HTML>");
		bwh.flush();
		bwh.close();
	}

	private int getTotal(int cp) {
		int count = 0;
		for (Entry<String, TreeSet<Integer>> obj : XMLFormattingHelper.getArMap().get(cp).entrySet()) {
			count += obj.getValue().size();
		}
		return count;
	}

	public static String timeDiff(long diff) {
		int diffDays = (int) (diff / (24 * 60 * 60 * 1000));
		String dateFormat = "";
		if (diffDays > 0) {
			dateFormat += diffDays + " day ";
		}
		diff -= diffDays * (24 * 60 * 60 * 1000);

		int diffhours = (int) (diff / (60 * 60 * 1000));
		if (diffhours > 0) {
			dateFormat += leftNumPadding(diffhours, 2) + " hour ";
		} else if (dateFormat.length() > 0) {
			dateFormat += "00 hour ";
		}
		diff -= diffhours * (60 * 60 * 1000);

		int diffmin = (int) (diff / (60 * 1000));
		if (diffmin > 0) {
			dateFormat += leftNumPadding(diffmin, 2) + " min ";
		} else if (dateFormat.length() > 0) {
			dateFormat += "00 min ";
		}

		diff -= diffmin * (60 * 1000);

		int diffsec = (int) (diff / (1000));
		if (diffsec > 0) {
			dateFormat += leftNumPadding(diffsec, 2) + " sec ";
		} else if (dateFormat.length() > 0) {
			dateFormat += "00 sec ";
		}

		int diffmsec = (int) (diff % (1000));
		dateFormat += leftNumPadding(diffmsec, 3) + " ms";
		return dateFormat;
	}

	private static String leftNumPadding(int str, int num) {
		return String.format("%0" + num + "d", str);
	}

	public static String rightPadding(String str, int num) {
		return String.format("%1$-" + num + "s", str);
	}
}
