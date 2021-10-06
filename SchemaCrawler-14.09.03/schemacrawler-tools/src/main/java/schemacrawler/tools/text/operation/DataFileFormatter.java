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
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.tools.options.TextOutputFormat;
import schemacrawler.tools.text.base.BaseXMLFormatter;
import schemacrawler.tools.text.utility.XMLFormattingHelper;
import schemacrawler.tools.traversal.DataTraversalHandler;
import schemacrawler.utility.Query;

/**
 * Text formatting of data.
 *
 * @author Malik
 */
final class DataFileFormatter extends BaseXMLFormatter<OperationOptions> implements DataTraversalHandler {

	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S");
	private static final int FIXED_SIZE = 175;

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
	DataFileFormatter(final Operation operation, final OperationOptions options, final OutputOptions outputOptions)
			throws SchemaCrawlerException {
		super(options, /* printVerboseDatabaseInfo */false, outputOptions);
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

	private void handleData(final String title, final ResultSet rows) throws SchemaCrawlerException {
		System.out.println("Processing " + title);
		if (rows == null) {
			return;
		}
		try {
			final DataResultSet dataRows = new DataResultSet(rows, options.isShowLobs(), options.isDateWithDateTime());
			iterateRows(dataRows, options.getBlobTableName());
		} catch (final SQLException | IOException e) {
			throw new SchemaCrawlerException(e.getMessage(), e);
		}
	}

	private static CharSequence numberFormatter(int i) {
		return String.format("%05d", i);
	}

	private void iterateRows(DataResultSet dataRows, String tablename) throws SQLException, IOException {
		Date startDate = new Date();
		System.out.println(rightPadding(tablename, 25) + " -- : " + "\t -> ResultSet gathered. XML writing started at "
				+ dateFormat.format(startDate));

		int recordsProcessed = 0;
		int counter = 0;

		final TextOutputFormat outputFormat = TextOutputFormat.valueOfFromString(outputOptions.getOutputFormatValue());
		final Path path = outputOptions.getOutputFile();

		String numberseq = numberFormatter(counter++).toString();
		new File(path.toString().replace("TEMP.xml", "") + "BLOBs" + File.separator
				+ checkValidFolder(options.getBlobTableName().toUpperCase()) + File.separator
				+ checkValidFolder(options.getBlobColumnName().toUpperCase()) + File.separator + "Folder-"
				+ options.getBlobFolderidentifier() + "-" + numberseq).mkdirs();

		String newPath = path.toString().replace("TEMP.xml", "").replace("TEMP.xml", "") + "tables" + File.separator
				+ (options.isVersion4() ? checkValidFile(options.getSchema().toUpperCase()) + "-" : "")
				+ checkValidFile(options.getBlobTableName().toUpperCase()) + "-BLOB-"
				+ checkValidFile(options.getBlobFolderidentifier()) + "-"
				+ checkValidFile(options.getBlobColumnName().toUpperCase()) + "-" + numberseq + ".xml";
		Path pathloc = Paths.get(newPath);
		outputOptions.setOutputFile(pathloc);
		out = new PrintWriter(outputOptions.openNewOutputWriter(options.isAppendOutput()), true);
		formattingHelper = new XMLFormattingHelper(out, outputFormat);

		formattingHelper.writeDocumentStart();
		if (options.isVersion4())
			formattingHelper.writeRootElementStart(options.getSchema());
		formattingHelper.writeRootElementStart(options.getBlobTableName() + "BLOB");
		formattingHelper.writeAttribute("BLOB_COLUMN", options.getBlobColumnName());
		/*
		 * formattingHelper.writeRootElementEnd(); if (options.isVersion4())
		 * formattingHelper.writeRootElementEnd(); formattingHelper.writeDocumentEnd();
		 * formattingHelper.flush();
		 */

		while (dataRows.next()) {

			if (recordsProcessed % options.getXmlChunkLimit() == 0 && recordsProcessed != 0) {
				numberseq = numberFormatter(counter++).toString();
				new File(path.toString().replace("TEMP.xml", "") + "BLOBs" + File.separator
						+ checkValidFolder(options.getBlobTableName().toUpperCase()) + File.separator
						+ checkValidFolder(options.getBlobColumnName().toUpperCase()) + File.separator + "Folder-"
						+ options.getBlobFolderidentifier() + "-" + numberseq).mkdirs();
				formattingHelper.writeRootElementEnd();
				if (options.isVersion4())
					formattingHelper.writeRootElementEnd();
				formattingHelper.writeDocumentEnd();
				out.flush();
				formattingHelper.flush();
				System.out.println(rightPadding(tablename, 25) + " -- : " + "\t --> " + recordsProcessed
						+ " record(s) processed. (Write Time Elapsed : "
						+ timeDiff(new Date().getTime() - startDate.getTime()) + ")");
				newPath = path.toString().replace("TEMP.xml", "").replace("TEMP.xml", "") + "tables" + File.separator
						+ (options.isVersion4() ? checkValidFile(options.getSchema().toUpperCase()) + "-" : "")
						+ checkValidFile(options.getBlobTableName().toUpperCase()) + "-BLOB-"
						+ checkValidFile(options.getBlobFolderidentifier()) + "-"
						+ checkValidFile(options.getBlobColumnName().toUpperCase()) + "-" + numberseq + ".xml";
				pathloc = Paths.get(newPath);
				outputOptions.setOutputFile(pathloc);
				out = new PrintWriter(outputOptions.openNewOutputWriter(options.isAppendOutput()), true);
				formattingHelper = new XMLFormattingHelper(out, outputFormat);
				formattingHelper.writeDocumentStart();
				if (options.isVersion4())
					formattingHelper.writeRootElementStart(options.getSchema());
				formattingHelper.writeRootElementStart(options.getBlobTableName() + "BLOB");
				formattingHelper.writeAttribute("BLOB_COLUMN", options.getBlobColumnName());
			}

			try {
				if (!options.getFilenameColumn().equalsIgnoreCase(""))
					processOutput(dataRows.getRows().getObject(1), dataRows.getRows().getObject(2),
							dataRows.getRows().getBlob(3), recordsProcessed, tablename, numberseq, path,
							recordsProcessed % options.getXmlChunkLimit());

				else if (!options.getFilenameOverrideValue().equalsIgnoreCase(""))
					processOutput(options.getFilenameOverrideValue(), dataRows.getRows().getObject(1),
							dataRows.getRows().getBlob(2), recordsProcessed, tablename, numberseq, path,
							recordsProcessed % options.getXmlChunkLimit());

				else
					processOutput(Integer.toString((options.getSeqStartValue() + recordsProcessed)),
							dataRows.getRows().getObject(1), dataRows.getRows().getBlob(2), recordsProcessed, tablename,
							numberseq, path, recordsProcessed % options.getXmlChunkLimit());
				recordsProcessed++;
			} catch (Exception e) {
				int seq = recordsProcessed;
				String filenamewithext = "unknown" + (++seq);

				if (options.isVersion4())
					formattingHelper.writeRecordStart("ROW", "");
				else
					formattingHelper.writeRecordStart(tablename, "-ROW");

				formattingHelper.writeElementStart("ID");
				formattingHelper.writeValue("REF_" + numberFormatterSeq(seq));
				formattingHelper.writeElementEnd();

				formattingHelper.writeElementStart("FILE");
				formattingHelper.writeValue(filenamewithext);
				formattingHelper.writeElementEnd();

				formattingHelper.writeElementStart("STATUS");
				formattingHelper.writeValue("unknown");
				formattingHelper.writeElementEnd();

				formattingHelper.writeElementStart("MESSAGE");
				formattingHelper.writeValue(e.getMessage());
				formattingHelper.writeElementEnd();

				formattingHelper.writeRecordEnd();
			}
			if (recordsProcessed % 100000 == 0)
				System.out.println(
						rightPadding(tablename, 25) + " -- : " + "\t --> " + recordsProcessed + " File extracted.");
			formattingHelper.flush();
		}

		if (recordsProcessed % options.getXmlChunkLimit() != 0 || recordsProcessed == 0) {
			formattingHelper.writeRootElementEnd();
			if (options.isVersion4())
				formattingHelper.writeRootElementEnd();
			formattingHelper.writeDocumentEnd();
			out.flush();
			formattingHelper.flush();
		}

		outputOptions.setRecordsProcessed(recordsProcessed);
		System.out.println(rightPadding(tablename, 25) + " -- : " + "\t Extraction Completed. Totally "
				+ recordsProcessed + " record(s) processed. (Total Write Time : "
				+ timeDiff(new Date().getTime() - startDate.getTime()) + ")");
	}

	private void processOutput(Object fileNameObj, Object extension, Blob blob, int seq, String tablename,
			String numberseq, Path path, int iterationCounter) {
		String filenamewithext = "unknown" + (++seq);
		if (blob == null) {

			if (options.isVersion4())
				formattingHelper.writeRecordStart("ROW", "");
			else
				formattingHelper.writeRecordStart(tablename, "-ROW");

			formattingHelper.writeElementStart("ID");
			formattingHelper.writeValue("REF_" + numberFormatterSeq(seq));
			formattingHelper.writeElementEnd();

			formattingHelper.writeElementStart("ORIGINAL_FILE");
			formattingHelper.writeValue(filenamewithext);
			formattingHelper.writeElementEnd();

			formattingHelper.writeElementStart("FILE");
			formattingHelper.writeValue(filenamewithext);
			formattingHelper.writeElementEnd();

			formattingHelper.writeElementStart("STATUS");
			formattingHelper.writeValue("unknown");
			formattingHelper.writeElementEnd();

			formattingHelper.writeElementStart("MESSAGE");
			formattingHelper.writeValue("Content was NULL");
			formattingHelper.writeElementEnd();

			formattingHelper.writeRecordEnd();
			return;
		}
		try {
			long startTime = new Date().getTime();
			InputStream in = blob.getBinaryStream();

			String ext = "";
			if (extension instanceof Clob) {
				Clob aclob = (Clob) extension;
				InputStream ip = aclob.getAsciiStream();
				for (int c = ip.read(); c != -1; c = ip.read()) {
					ext += (char) c;
				}
				ip.close();
			} else if (extension instanceof NClob) {
				NClob aclob = (NClob) extension;
				InputStream ip = aclob.getAsciiStream();
				for (int c = ip.read(); c != -1; c = ip.read()) {
					ext += (char) c;
				}
				ip.close();
			} else {
				ext = extension == null ? "" : extension.toString().trim();
			}

			String filename = "";
			if (fileNameObj instanceof Clob) {
				Clob aclob = (Clob) fileNameObj;
				InputStream ip = aclob.getAsciiStream();
				for (int c = ip.read(); c != -1; c = ip.read()) {
					filename += (char) c;
				}
				ip.close();
			} else if (fileNameObj instanceof NClob) {
				NClob aclob = (NClob) fileNameObj;
				InputStream ip = aclob.getAsciiStream();
				for (int c = ip.read(); c != -1; c = ip.read()) {
					filename += (char) c;
				}
				ip.close();
			} else {
				filename = fileNameObj == null ? "unknown" : fileNameObj.toString().trim();
			}

			while (filename.toString().endsWith(".")) {
				filename = filename.toString().endsWith(".")
						? filename.toString().substring(0, filename.toString().length() - 1)
						: filename;
			}

			while (ext.startsWith(".")) {
				ext = ext.startsWith(".") ? ext.substring(1) : ext;
			}

			filenamewithext = filename.toString() + ((!ext.equals("")) ? "." : "") + ext;
			String validfilename = checkValidFile(filenamewithext);

			String getExt = getFileExtension(validfilename);
			if (getExt == null)
				validfilename = validfilename + "_" + String.format("%06d", iterationCounter);
			else
				validfilename = truncateName(validfilename.substring(0, validfilename.lastIndexOf(getExt) - 1)) + "_"
						+ String.format("%06d", iterationCounter) + "." + getExt;

			String file = path.toString().replace("TEMP.xml", "") + "BLOBs" + File.separator
					+ checkValidFolder(options.getBlobTableName().toUpperCase()) + File.separator
					+ checkValidFolder(options.getBlobColumnName().toUpperCase()) + File.separator + "Folder-"
					+ options.getBlobFolderidentifier() + "-" + numberseq + File.separator + validfilename;

			OutputStream out = new FileOutputStream(file);
			byte[] buff = new byte[4096];
			int len = 0;
			while ((len = in.read(buff)) != -1) {
				out.write(buff, 0, len);
			}
			out.flush();
			out.close();

			boolean sizeexp = false;
			double size = 0;
			try {
				size = Math.ceil(((double) (new File(file).length()) / (double) 1024));
			} catch (Exception e) {
				sizeexp = true;
			}
			long endTime = new Date().getTime();

			if (options.isVersion4())
				formattingHelper.writeRecordStart("ROW", "");
			else
				formattingHelper.writeRecordStart(tablename, "-ROW");

			formattingHelper.writeElementStart("ID");
			formattingHelper.writeValue("REF_" + numberFormatterSeq(seq));
			formattingHelper.writeElementEnd();
			formattingHelper.writeElementStart("ORIGINAL_FILE");
			formattingHelper.writeValue(filenamewithext);
			formattingHelper.writeElementEnd();
			formattingHelper.writeElementStart("FILE");
			formattingHelper.writeAttribute("ref",
					"../BLOBs/" + checkValidFolder(options.getBlobTableName().toUpperCase()) + "/"
							+ checkValidFolder(options.getBlobColumnName().toUpperCase()) + "/" + "Folder-"
							+ options.getBlobFolderidentifier() + "-" + numberseq + "/" + validfilename);
			formattingHelper.writeValue(validfilename);
			formattingHelper.writeElementEnd();
			formattingHelper.writeElementStart("STATUS");
			formattingHelper.writeAttribute("SIZE_KB", sizeexp ? "Could not determine" : Double.toString(size));
			formattingHelper.writeAttribute("TIME_MS", Long.toString(endTime - startTime));
			formattingHelper.writeValue("SUCCESS");
			formattingHelper.writeElementEnd();
			formattingHelper.writeRecordEnd();
		} catch (Exception e) {
			e.printStackTrace();

			if (options.isVersion4())
				formattingHelper.writeRecordStart("ROW", "");
			else
				formattingHelper.writeRecordStart(tablename, "-ROW");

			formattingHelper.writeElementStart("ID");
			formattingHelper.writeValue("REF_" + numberFormatterSeq(seq));
			formattingHelper.writeElementEnd();
			formattingHelper.writeElementStart("ORIGINAL_FILE");
			formattingHelper.writeValue(filenamewithext);
			formattingHelper.writeElementEnd();
			formattingHelper.writeElementStart("FILE");
			formattingHelper.writeValue(filenamewithext);
			formattingHelper.writeElementEnd();
			formattingHelper.writeElementStart("STATUS");
			formattingHelper.writeValue("FAILURE");
			formattingHelper.writeElementEnd();
			formattingHelper.writeElementStart("MESSAGE");
			formattingHelper.writeValue(e.getMessage());
			formattingHelper.writeElementEnd();
			formattingHelper.writeRecordEnd();
		}
	}

	private String truncateName(String text) {
		if (text == null)
			return "unnamed";
		if (text.length() >= FIXED_SIZE)
			return text.substring(0, FIXED_SIZE - 1);
		return text;
	}

	private static CharSequence numberFormatterSeq(int i) {
		return String.format("%010d", i);
	}

	public static String rightPadding(String str, int num) {
		return String.format("%1$-" + num + "s", str);
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

	public static boolean hasValue(String s) {
		return (s != null) && (s.trim().length() > 0);
	}

	public static String getFileExtension(String filename) {
		if (!hasValue(filename))
			return null;
		int index = filename.lastIndexOf('.');
		if (index == -1)
			return null;
		return filename.substring(index + 1, filename.length());
	}
}
