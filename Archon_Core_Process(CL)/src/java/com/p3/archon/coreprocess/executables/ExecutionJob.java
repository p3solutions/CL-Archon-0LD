package com.p3.archon.coreprocess.executables;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;
import org.xhtmlrenderer.pdf.ITextRenderer;

import com.itextpdf.text.Image;
import com.itextpdf.text.pdf.PdfContentByte;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfStamper;
import com.p3.archon.coreprocess.beans.ArchonInputBean;
import com.p3.archon.coreprocess.common.Constants;
import com.p3.archon.utils.FileUtil;

import schemacrawler.schemacrawler.ExcludeAll;
import schemacrawler.schemacrawler.IncludeAll;
import schemacrawler.schemacrawler.RegularExpressionInclusionRule;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaInfoLevel;

public class ExecutionJob {

	protected static final String SPLIT = ";";
	protected static final String SEPERATOR = ",";
	protected DataSource dataSource;
	protected Connection connection;
	protected ArchonInputBean inputArgs;

	public ExecutionJob(DataSource dataSource, Connection connection, ArchonInputBean inputArgs) {
		this.dataSource = dataSource;
		this.connection = connection;
		this.inputArgs = inputArgs;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	public ArchonInputBean getInputArgs() {
		return inputArgs;
	}

	public void setInputArgs(ArchonInputBean inputArgs) {
		this.inputArgs = inputArgs;
	}

	protected void printLines(String[] textLines) {
		for (String line : textLines) {
			System.out.println(line);
		}
	}

	protected SchemaCrawlerOptions schemaCrawlerOptionsBuilder(ArchonInputBean inputArgs, boolean hideEmptyTables,
			SchemaInfoLevel schemaInfoLevel, boolean columnInclusion, boolean routineInclusion,
			boolean routineColumnInclusion) {
		SchemaCrawlerOptions schemaCrawlerOptions = new SchemaCrawlerOptions();
		schemaCrawlerOptions.setHideEmptyTables(hideEmptyTables);
		schemaCrawlerOptions.setSchemaInfoLevel(schemaInfoLevel);
		schemaCrawlerOptions.setColumnInclusionRule(columnInclusion ? new IncludeAll() : new ExcludeAll());
		schemaCrawlerOptions.setRoutineInclusionRule(routineInclusion ? new IncludeAll() : new ExcludeAll());
		schemaCrawlerOptions
				.setRoutineColumnInclusionRule(routineColumnInclusion ? new IncludeAll() : new ExcludeAll());

		switch (inputArgs.getDatabaseServer().toLowerCase()) {
		case "sql":
		case "sqlwinauth":
		case "sybase":
			schemaCrawlerOptions.setSchemaInclusionRule(new RegularExpressionInclusionRule(inputArgs.databaseName + "."
					+ (inputArgs.schemaName != null && !inputArgs.schemaName.trim().equals("") ? inputArgs.schemaName.trim()
							: "*")));
			schemaCrawlerOptions.setTableInclusionRule(new RegularExpressionInclusionRule(inputArgs.databaseName + "."
					+ (inputArgs.schemaName != null && !inputArgs.schemaName.trim().equals("") ? inputArgs.schemaName.trim() : "*")
					+ "."
					+ (inputArgs.tableInclusionRule != null && !inputArgs.tableInclusionRule.trim().equals("") ? inputArgs.tableInclusionRule.trim() : "*")));
			break;
		case "teradata":
		case "oracle":
		case "oracleservice":
		case "mysql":
		case "maria":
		case "mariadb":
		case "db2":
		case "postgresql":
		case "as400":
		case "as400noport":
			if (inputArgs.getSchema() != null && !inputArgs.getSchema().trim().equals("")) {
				schemaCrawlerOptions.setSchemaInclusionRule(new RegularExpressionInclusionRule(inputArgs.getSchema()));
				schemaCrawlerOptions.setTableInclusionRule(new RegularExpressionInclusionRule(inputArgs.schemaName + "."
						+ (inputArgs.tableInclusionRule != null && !inputArgs.tableInclusionRule.trim().equals("") ? inputArgs.tableInclusionRule.trim()
								: "*")));
			}
			break;
		default:
			schemaCrawlerOptions.setTableInclusionRule(new RegularExpressionInclusionRule("*"));
		}

		Collection<String> tableTypes = new ArrayList<String>();
		if (Constants.NEED_TABLES)
			tableTypes.add("Table");
		if (Constants.NEED_VIEWS)
			tableTypes.add("View");

		for (String tabletype : Constants.ADDITIONAL_TABLE_TYPE) {
			tableTypes.add(tabletype);
		}

		schemaCrawlerOptions.setTableTypes(tableTypes);

		return schemaCrawlerOptions;
	}

	protected static Object[] prepareObjectArray(int i) {
		Object[] array = new Object[i];
		for (int j = 0; j < array.length; j++) {
			array[j] = "--------------------------------------------------------------------------------------------------------------------"
					.substring(0, lengthArray[j]);

		}
		return array;
	}

	protected static Object[] prepareObjectArrayBlobInfo(int i) {
		Object[] array = new Object[i];
		for (int j = 0; j < array.length; j++) {
			array[j] = ("---------------------------------------------------------------------------------------------------------"
					+ "----------------------------------------------------------------------------------------------------------"
					+ "----------------------------------------------------------------------------------------------------------"
					+ "----------------------------------------------------------------------------------------------------------"
					+ "----------------------------------------------------------------------------------------------------------"
					+ "----------------------------------------------------------------------------------------------------------"
					+ "----------------------------------------------------------------------------------------------------------"
					+ "-----------------------------").substring(0, lengthArrayBlobInfo[j]);

		}
		return array;
	}

	protected static String escapeAndQuoteCsv(final String text) {
		final char QUOTE = '\"';
		final char SEPARATOR = ',';
		final char NEWLINE = '\n';

		final String value = String.valueOf(text);
		final int length = value.length();
		if (length == 0) {
			return "\"\"";
		}

		if (value.indexOf(SEPARATOR) < 0 && value.indexOf(QUOTE) < 0 && value.indexOf(NEWLINE) < 0) {
			return value;
		}

		final StringBuilder sb = new StringBuilder(length);
		sb.append(QUOTE);
		for (int i = 0; i < length; i++) {
			final char c = value.charAt(i);
			if (c == QUOTE) {
				sb.append(QUOTE).append(c);
			} else {
				sb.append(c);
			}
		}
		sb.append(QUOTE);

		return sb.toString();
	}

	protected static Object[] prepareObjectArrayError(int i) {
		Object[] array = new Object[i];
		for (int j = 0; j < array.length; j++) {
			array[j] = ("---------------------------------------------------------------------------------------------------------"
					+ "----------------------------------------------------------------------------------------------------------"
					+ "----------------------------------------------------------------------------------------------------------"
					+ "----------------------------------------------------------------------------------------------------------"
					+ "----------------------------------------------------------------------------------------------------------"
					+ "----------------------------------------------------------------------------------------------------------"
					+ "----------------------------------------------------------------------------------------------------------"
					+ "-----------------------------").substring(0, lengthArrayError[j]);

		}
		return array;
	}

	static int[] lengthArray = new int[] { 40, 30, 30, 25, 25, 18, 43 };
	static int[] lengthArrayError = new int[] { 40, 500 };
	static int[] lengthArrayBlobInfo = new int[] { 50, 50, 40, 40 };

	protected static void viewFormatter(Object[] strings, boolean flag) {
		System.out.print(flag ? "+" : "|");
		for (int i = 0; i < strings.length; i++) {
			System.out.print(rightPadding(strings[i] + "", lengthArray[i]));
			System.out.print(flag ? "+" : "|");
		}
		System.out.println();
	}

	protected static void viewFormatterError(Object[] strings, boolean flag) {
		System.out.print(flag ? "+" : "|");
		for (int i = 0; i < strings.length; i++) {
			System.out.print(rightPadding(strings[i] + "", lengthArrayError[i]));
			System.out.print(flag ? "+" : "|");
		}
		System.out.println();
	}

	protected static void viewFormatterBlobInfo(Object[] strings, boolean flag) {
		System.out.print(flag ? "+" : "|");
		for (int i = 0; i < strings.length; i++) {
			System.out.print(rightPadding(strings[i] + "", lengthArrayBlobInfo[i]));
			System.out.print(flag ? "+" : "|");
		}
		System.out.println();
	}

	protected static StringBuffer createHeader(StringBuffer sb, ArchonInputBean inputArgs, String query) {
		StringBuffer sfb = new StringBuffer();
		sfb.append(
				"<html><head><style> @page {  size: 18in 12in; } table {  background-color: transparent;}caption {  padding-top: 8px;  padding-bottom: 8px;  color: #777;  text-align: left;}th {  text-align: left;}.table {  width: 100%;  max-width: 100%;  margin-bottom: 20px;}.table > thead > tr > th,.table > tbody > tr > th,.table > tfoot > tr > th,.table > thead > tr > td,.table > tbody > tr > td,.table > tfoot > tr > td {  padding: 8px;  line-height: 1.42857143;  vertical-align: top;  border-top: 1px solid #ddd;}.table > thead > tr > th {  vertical-align: bottom;  border-bottom: 2px solid #ddd;}.table > caption + thead > tr:first-child > th,.table > colgroup + thead > tr:first-child > th,.table > thead:first-child > tr:first-child > th,.table > caption + thead > tr:first-child > td,.table > colgroup + thead > tr:first-child > td,.table > thead:first-child > tr:first-child > td {  border-top: 0;}.table > tbody + tbody {  border-top: 2px solid #ddd;}.table .table {  background-color: #fff;}.table-condensed > thead > tr > th,.table-condensed > tbody > tr > th,.table-condensed > tfoot > tr > th,.table-condensed > thead > tr > td,.table-condensed > tbody > tr > td,.table-condensed > tfoot > tr > td {  padding: 5px;}.table-bordered {  border: 1px solid #ddd;}.table-bordered > thead > tr > th,.table-bordered > tbody > tr > th,.table-bordered > tfoot > tr > th,.table-bordered > thead > tr > td,.table-bordered > tbody > tr > td,.table-bordered > tfoot > tr > td {  border: 1px solid #ddd;}.table-bordered > thead > tr > th,.table-bordered > thead > tr > td {  border-bottom-width: 2px;}.table-striped > tbody > tr:nth-of-type(odd) {  background-color: #f9f9f9;}.table-hover > tbody > tr:hover {  background-color: #f5f5f5;}table col[class*=\"col-\"] {  position: static;  display: table-column;  float: none;}table td[class*=\"col-\"],table th[class*=\"col-\"] {  position: static;  display: table-cell;  float: none;}.table > thead > tr > td.active,.table > tbody > tr > td.active,.table > tfoot > tr > td.active,.table > thead > tr > th.active,.table > tbody > tr > th.active,.table > tfoot > tr > th.active,.table > thead > tr.active > td,.table > tbody > tr.active > td,.table > tfoot > tr.active > td,.table > thead > tr.active > th,.table > tbody > tr.active > th,.table > tfoot > tr.active > th {  background-color: #f5f5f5;}.table-hover > tbody > tr > td.active:hover,.table-hover > tbody > tr > th.active:hover,.table-hover > tbody > tr.active:hover > td,.table-hover > tbody > tr:hover > .active,.table-hover > tbody > tr.active:hover > th {  background-color: #e8e8e8;}.table > thead > tr > td.success,.table > tbody > tr > td.success,.table > tfoot > tr > td.success,.table > thead > tr > th.success,.table > tbody > tr > th.success,.table > tfoot > tr > th.success,.table > thead > tr.success > td,.table > tbody > tr.success > td,.table > tfoot > tr.success > td,.table > thead > tr.success > th,.table > tbody > tr.success > th,.table > tfoot > tr.success > th {  background-color: #dff0d8;}.table-hover > tbody > tr > td.success:hover,.table-hover > tbody > tr > th.success:hover,.table-hover > tbody > tr.success:hover > td,.table-hover > tbody > tr:hover > .success,.table-hover > tbody > tr.success:hover > th {  background-color: #d0e9c6;}.table > thead > tr > td.info,.table > tbody > tr > td.info,.table > tfoot > tr > td.info,.table > thead > tr > th.info,.table > tbody > tr > th.info,.table > tfoot > tr > th.info,.table > thead > tr.info > td,.table > tbody > tr.info > td,.table > tfoot > tr.info > td,.table > thead > tr.info > th,.table > tbody > tr.info > th,.table > tfoot > tr.info > th {  background-color: #d9edf7;}.table-hover > tbody > tr > td.info:hover,.table-hover > tbody > tr > th.info:hover,.table-hover > tbody > tr.info:hover > td,.table-hover > tbody > tr:hover > .info,.table-hover > tbody > tr.info:hover > th {  background-color: #c4e3f3;}.table > thead > tr > td.warning,.table > tbody > tr > td.warning,.table > tfoot > tr > td.warning,.table > thead > tr > th.warning,.table > tbody > tr > th.warning,.table > tfoot > tr > th.warning,.table > thead > tr.warning > td,.table > tbody > tr.warning > td,.table > tfoot > tr.warning > td,.table > thead > tr.warning > th,.table > tbody > tr.warning > th,.table > tfoot > tr.warning > th {  background-color: #fcf8e3;}.table-hover > tbody > tr > td.warning:hover,.table-hover > tbody > tr > th.warning:hover,.table-hover > tbody > tr.warning:hover > td,.table-hover > tbody > tr:hover > .warning,.table-hover > tbody > tr.warning:hover > th {  background-color: #faf2cc;}.table > thead > tr > td.danger,.table > tbody > tr > td.danger,.table > tfoot > tr > td.danger,.table > thead > tr > th.danger,.table > tbody > tr > th.danger,.table > tfoot > tr > th.danger,.table > thead > tr.danger > td,.table > tbody > tr.danger > td,.table > tfoot > tr.danger > td,.table > thead > tr.danger > th,.table > tbody > tr.danger > th,.table > tfoot > tr.danger > th {  background-color: #f2dede;}.table-hover > tbody > tr > td.danger:hover,.table-hover > tbody > tr > th.danger:hover,.table-hover > tbody > tr.danger:hover > td,.table-hover > tbody > tr:hover > .danger,.table-hover > tbody > tr.danger:hover > th {  background-color: #ebcccc;}.table-responsive {  min-height: .01%;  overflow-x: auto;}@media screen and (max-width: 767px) {  .table-responsive {    width: 100%;    margin-bottom: 15px;    overflow-y: hidden;    -ms-overflow-style: -ms-autohiding-scrollbar;    border: 1px solid #ddd;  }  .table-responsive > .table {    margin-bottom: 0;  }  .table-responsive > .table > thead > tr > th,  .table-responsive > .table > tbody > tr > th,  .table-responsive > .table > tfoot > tr > th,  .table-responsive > .table > thead > tr > td,  .table-responsive > .table > tbody > tr > td,  .table-responsive > .table > tfoot > tr > td {    white-space: nowrap;  }  .table-responsive > .table-bordered {    border: 0;  }  .table-responsive > .table-bordered > thead > tr > th:first-child,  .table-responsive > .table-bordered > tbody > tr > th:first-child,  .table-responsive > .table-bordered > tfoot > tr > th:first-child,  .table-responsive > .table-bordered > thead > tr > td:first-child,  .table-responsive > .table-bordered > tbody > tr > td:first-child,  .table-responsive > .table-bordered > tfoot > tr > td:first-child {    border-left: 0;  }  .table-responsive > .table-bordered > thead > tr > th:last-child,  .table-responsive > .table-bordered > tbody > tr > th:last-child,  .table-responsive > .table-bordered > tfoot > tr > th:last-child,  .table-responsive > .table-bordered > thead > tr > td:last-child,  .table-responsive > .table-bordered > tbody > tr > td:last-child,  .table-responsive > .table-bordered > tfoot > tr > td:last-child {    border-right: 0;  }  .table-responsive > .table-bordered > tbody > tr:last-child > th,  .table-responsive > .table-bordered > tfoot > tr:last-child > th,  .table-responsive > .table-bordered > tbody > tr:last-child > td,  .table-responsive > .table-bordered > tfoot > tr:last-child > td {    border-bottom: 0;  }}</style></head><body>");

		sfb.append(getTitle(false));
		sfb.append("<br></br><table class=\"table table-bordered table-striped\"><thead><tr>");
		sfb.append("<td colspan=\"1\" align=\"center\"><b>Paramerter</b></td>");
		sfb.append("<td colspan=\"3\" align=\"center\"><b>Value</b></td>");
		sfb.append("</tr></thead><tbody>");
		sfb.append("<tr><td colspan=\"1\"><b>" + "Database Server" + "</b></td><td colspan=\"3\">"
				+ inputArgs.getDatabaseServer() + "</td></tr>");
		sfb.append("<tr><td colspan=\"1\"><b>" + "Host" + "</b></td><td colspan=\"3\">" + inputArgs.getHost()
				+ "</td></tr>");
		sfb.append("<tr><td colspan=\"1\"><b>" + "Port" + "</b></td><td colspan=\"3\">" + inputArgs.getPort()
				+ "</td></tr>");
		sfb.append("<tr><td colspan=\"1\"><b>" + "Database" + "</b></td><td colspan=\"3\">" + inputArgs.getDatabase()
				+ "</td></tr>");
		sfb.append("<tr><td colspan=\"1\"><b>" + "Schema" + "</b></td><td colspan=\"3\">" + inputArgs.getSchema()
				+ "</td></tr>");
		sfb.append("</tbody></table>");
		if (query != null) {
			sfb.append("<br></br><table class=\"table table-bordered table-striped\"><thead><tr>");
			sfb.append("<td colspan=\"1\" align=\"center\"><b>Query Title</b></td>");
			sfb.append("<td colspan=\"3\" align=\"center\"><b>Query</b></td>");
			sfb.append("</tr></thead><tbody>");
			for (String str : query.split("\n")) {
				String line[] = str.split(":QUERY:");
				if (line.length == 2) {
					sfb.append("<tr><td colspan=\"1\"><b>" + line[0] + "</b></td><td colspan=\"3\">"
							+ (line[1] != null ? (line[1].replace(">", "&gt;").replace("<", "&lt;")) : "")
							+ "</td></tr>");
				} else {
					sfb.append("<tr><td colspan=\"1\"><b>" + line[0] + "</b></td><td colspan=\"3\">"
							+ "Query not printable on report" + "</td></tr>");
				}

			}
			sfb.append("</tbody></table>");
		}
		sfb.append(sb.toString());

		return sfb;
	}

	protected static String getTitle(boolean isSummaryReport) {
		if (isSummaryReport)
			return ("<b><font style=\"size:14px\">Extraction Summary</font></b><br/>");
		else
			return ("<center><b><font style=\"size:14px\">Archon Extraction Report</font></b></center>");
	}

	protected static StringBuffer createTable(StringBuffer sb, Object[] strings, boolean flag, boolean info,
			boolean error, boolean blobinfo) {
		if (flag) {
			sb.append("<br></br>");
			if (info) {
				sb.append("<br></br>" + "<center><b><font style=\"size:14px\">Extraction Info</font></b></center>"
						+ "<br></br><table class=\"table table-bordered table-striped\"><thead><tr>");
				sb.append("<td colspan=\"3\" align=\"center\"><b>" + strings[0] + "</b></td>");
				sb.append("<td colspan=\"2\" align=\"center\"><b>" + strings[1] + "</b></td>");
				sb.append("<td colspan=\"2\" align=\"center\"><b>" + strings[2] + "</b></td>");
				sb.append("<td colspan=\"1\" align=\"center\"><b>" + strings[3] + "</b></td>");
				sb.append("<td colspan=\"1\" align=\"center\"><b>" + strings[4] + "</b></td>");
				sb.append("<td colspan=\"1\" align=\"center\"><b>" + strings[5] + "</b></td>");
				sb.append("<td colspan=\"2\" align=\"center\"><b>" + strings[6] + "</b></td>");
			} else if (error) {
				sb.append("<br></br>" + "<center><b><font style=\"size:14px\">Error Info</font></b></center>"
						+ "<br></br><table class=\"table table-bordered table-striped\"><thead><tr>");
				sb.append("<td colspan=\"3\" align=\"center\"><b>" + strings[0] + "</b></td>");
				sb.append("<td colspan=\"5\" align=\"center\"><b>" + strings[1] + "</b></td>");
			} else if (blobinfo) {
				sb.append("<br></br>" + "<center><b><font style=\"size:14px\">Blob Info</font></b></center>"
						+ "<br></br><table class=\"table table-bordered table-striped\"><thead><tr>");
				sb.append("<td colspan=\"1\" align=\"center\"><b>" + strings[0] + "</b></td>");
				sb.append("<td colspan=\"1\" align=\"center\"><b>" + strings[1] + "</b></td>");
				sb.append("<td colspan=\"1\" align=\"center\"><b>" + strings[2] + "</b></td>");
				sb.append("<td colspan=\"1\" align=\"center\"><b>" + strings[3] + "</b></td>");
			}
			sb.append("</tr></thead><tbody>");
		} else {
			sb.append("</tbody></table>");
		}
		return sb;
	}

	@SuppressWarnings("deprecation")
	protected static StringBuffer createFooter(StringBuffer blobinfo, StringBuffer error, StringBuffer sb,
			Object[] strings, String reportId) {
		sb.append(blobinfo.toString());
		sb.append(error.toString());
		sb.append("<br></br>");
		sb.append("<br></br>");
		sb.append("<br></br>");
		sb.append("<br></br>");
		if (reportId != null)
			sb.append("<p>Job Reference Id : " + reportId + "</p>");
		sb.append("<p>Generated with Archon</p>");
		sb.append("<p>Generated Date : " + new Date().toGMTString() + "</p>");
		sb.append("<p>Job Execution Time : " + strings[0] + "</p>");
		sb.append("<p><i><font size=\"8px\">This is a system generated report.</font></i></p>");
		sb.append("</body></html>");
		return sb;
	}

	protected static StringBuffer createData(StringBuffer sb, Object[] strings, boolean info, boolean error,
			boolean blobinfo) {
		if (info) {
			sb.append("<tr>");
			sb.append("<td colspan=\"3\" align=\"left\">" + strings[0] + "</td>");
			sb.append("<td colspan=\"2\" align=\"center\">" + strings[1] + "</td>");
			sb.append("<td colspan=\"2\" align=\"center\">" + strings[2] + "</td>");
			sb.append("<td colspan=\"1\" align=\"right\">" + strings[3] + "</td>");
			sb.append("<td colspan=\"1\" align=\"right\">" + strings[4] + "</td>");

			if (strings[5].toString().equalsIgnoreCase("error"))
				sb.append("<td colspan=\"1\" align=\"center\"><b><font color=\"red\">ERROR</font></b></td>");
			else if (strings[5].toString().equalsIgnoreCase("true"))
				sb.append("<td colspan=\"1\" align=\"center\"><b><font color=\"green\">YES</font></b></td>");
			else
				sb.append("<td colspan=\"1\" align=\"center\"><b><font color=\"red\">NO</font></b></td>");
			sb.append("<td colspan=\"2\" align=\"right\">" + strings[6] + "</td>");
			sb.append("</tr>");
			return sb;
		} else if (error) {
			sb.append("<tr>");
			sb.append("<td colspan=\"3\" align=\"left\">" + strings[0] + "</td>");
			sb.append("<td colspan=\"5\" align=\"left\">" + strings[1].toString().replace("\n", " ") + "</td>");
			sb.append("</tr>");
			return sb;
		} else if (blobinfo) {
			sb.append("<tr>");
			sb.append("<td colspan=\"1\" align=\"left\">" + strings[0] + "</td>");
			sb.append("<td colspan=\"1\" align=\"left\">" + strings[1] + "</td>");
			sb.append("<td colspan=\"1\" align=\"right\">" + strings[2] + "</td>");
			sb.append("<td colspan=\"1\" align=\"right\">" + strings[3] + "</td>");
			sb.append("</tr>");
			return sb;
		} else
			return sb;
	}

	protected void generateSummaryReport(StringBuffer sb, StringBuffer error, String generateSummaryReportPath,
			String reportId) {
		try {
			Writer writer = new OutputStreamWriter(new FileOutputStream(generateSummaryReportPath, true));
			writer.write(getTitle(true));
			writer.write(sb.toString());
			writer.write("<br></br>");
			writer.write((error.length() == 0) ? "" : error.toString());
			writer.flush();
			writer.close();
			if (Constants.MOVE_FILE_TO_NAS)
				FileUtil.movefile(generateSummaryReportPath, Constants.NAS_FILE_PATH, false);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected void createReport(StringBuffer sb, String path, String reportId) {
		try {
			String inputfile = path + File.separator + "out.html";
			String tempfile = path + File.separator + "temp.pdf";
			String finalfile = path + File.separator + "Extraction Report_"
					+ (reportId != null ? reportId.substring(0, 8) : UUID.randomUUID().toString().substring(0, 8))
					+ ".pdf";
			PrintWriter writer = new PrintWriter(inputfile, "UTF-8");
			writer.println(sb.toString());
			writer.close();

			String url = new File(inputfile).toURI().toURL().toString();
			OutputStream os = new FileOutputStream(new File(tempfile));
			ITextRenderer renderer = new ITextRenderer();
			renderer.setDocument(url);
			renderer.layout();
			renderer.createPDF(os);
			os.close();

			FileInputStream is = new FileInputStream(tempfile);
			PdfReader pdfReader = new PdfReader(is);
			FileOutputStream os2 = new FileOutputStream(new File(finalfile));
			PdfStamper pdfStamper = new PdfStamper(pdfReader, os2);
			Image image = Image.getInstance("archon.png");
			image.scaleToFit(300f, 300f);
			image.setAbsolutePosition(950f, 20f);
			for (int i = 1; i <= pdfReader.getNumberOfPages(); i++) {
				PdfContentByte content = pdfStamper.getUnderContent(i);
				content.addImage(image);
			}
			pdfStamper.close();
			pdfReader.close();
			if (Constants.MOVE_FILE_TO_NAS)
				FileUtil.movefile(finalfile, Constants.NAS_FILE_PATH, false);
			is.close();
			os2.close();
			FileUtils.forceDeleteOnExit(new File(inputfile));
			FileUtils.forceDeleteOnExit(new File(tempfile));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String rightPadding(String str, int num) {
		return String.format("%1$-" + num + "s", str);
	}

	public static String leftPadding(String str, int num) {
		return "                                                         ".substring(0, (num - str.length())) + str;
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

	public void start() throws Exception {
	}

	public static boolean deleteDirectory(File dir) {
		if (dir == null)
			return true;
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (String element : children) {
				boolean success = deleteDirectory(new File(dir, element));
				if (!success) {
					System.err.println("Unable to delete file: " + new File(dir, element));
					return false;
				}
			}
		}

		// The directory is now empty so delete it
		return dir.delete();
	}
}
