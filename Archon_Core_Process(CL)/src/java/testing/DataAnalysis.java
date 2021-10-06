package testing;

import static us.fatehi.commandlineparser.CommandLineUtility.applyApplicationLogLevel;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;

import javax.sql.DataSource;

import com.p3.archon.commonfunctions.CommonFunctions;

import schemacrawler.schema.Catalog;
import schemacrawler.schema.Column;
import schemacrawler.schema.Table;
import schemacrawler.schema.View;
import schemacrawler.schemacrawler.DatabaseConnectionOptions;
import schemacrawler.schemacrawler.ExcludeAll;
import schemacrawler.schemacrawler.RegularExpressionInclusionRule;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.tools.analysis.counts.CatalogWithCounts;
import schemacrawler.tools.analysis.counts.CountsUtility;
import schemacrawler.utility.SchemaCrawlerUtility;

public class DataAnalysis {

	static String file = "analysis_" + new Date().getTime() + ".csv";
	static Writer out;

	static String SERVER = "";
	static String HOST = "";
	static String PORT = "";
	static String DB = "";
	static String SCHEMA = "";
	static String USER = "";
	static String PASS = "";
	static String recordCount = "";
	static int MAX_THREAD = 1000;
	static int threadCounter = 0;

	public static void main(final String[] args) throws Exception {

		if (args.length != 9) {
			System.out.println(
					"Usage input paramteres : <SERVER (eg. SQL,ORACLE)> <HOST IP> <PORT> <DB> <SCHEMA> <USERNAME> <PASSWORD> <RECORDCOUNT (eg. 100, 200, 500, 1000)> <MAX THREADS integer>");
			System.exit(1);
		}

		SERVER = args[0];
		HOST = args[1];
		PORT = args[2];
		DB = args[3];
		SCHEMA = args[4];
		USER = args[5];
		PASS = args[6];
		recordCount = args[7];
		try{
			MAX_THREAD = Integer.parseInt(args[8]);
		}
		catch(Exception e){
			MAX_THREAD = 1000;
		}

		System.out.println("Analysis Started " + new Date());
		applyApplicationLogLevel(Level.OFF);

		final DataSource dataSource = new DatabaseConnectionOptions(getURL());
		final Connection connection = dataSource.getConnection(USER, PASS);
		final SchemaCrawlerOptions options = new SchemaCrawlerOptions();
		options.setRoutineInclusionRule(new ExcludeAll());
		options.setSchemaInclusionRule(new RegularExpressionInclusionRule(getSchemaSelection()));
		Catalog xcatalog = SchemaCrawlerUtility.getCatalog(connection, options);
		final Catalog catalog = new CatalogWithCounts(xcatalog, connection, options);

		final List<Table> allTables = new ArrayList<>(catalog.getTables());

		if (allTables.size() > 0) {
			out = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
			out.write("Primary Table,Primary Column, Comparing Table, Comparing Column, Accuracy\n");
			out.flush();
		} else {
			System.out.println("No Table available to Analyse");
			System.out.println("Analysis Terminated as there were no table " + new Date());
			System.exit(1);
		}

		int i = 0;
		while (i < (allTables.size() - 1)) {
			Table primarytable = allTables.get(i);
			System.out.println(" primary table : " + primarytable.getName());
			i++;
			if (primarytable instanceof View) {
				continue;
			}
			if (CountsUtility.getRowCountMessage(primarytable).equalsIgnoreCase("Empty")) {
				System.out.println("Primay Table : " + primarytable.getFullName() + " is empty. Skipping Analysis");
				continue;
			}
			List<Column> primeCol = primarytable.getColumns();
			int j = i + 1;
			while (j < allTables.size()) {
				Table comparingtable = allTables.get(j);
				System.out.println("\t comparing table : " + comparingtable.getName());
				j++;
				if (comparingtable instanceof View) {
					continue;
				}
				if (CountsUtility.getRowCountMessage(comparingtable).equalsIgnoreCase("Empty")) {
					System.out
							.println("Comparing Table : " + comparingtable.getFullName() + " is empty. Skipping Table");
					continue;
				}
				List<Column> compCol = comparingtable.getColumns();
				for (Column p : primeCol) {
					System.out.println("\t\t p column : " + p.getName());
					if (isBlob(p.getColumnDataType().getJavaSqlType().getJavaSqlType()))
						continue;
					for (Column c : compCol) {
						System.out.println("\t\t\t c column: " + c.getName());
						if (isBlob(c.getColumnDataType().getJavaSqlType().getJavaSqlType()))
							continue;
						if (!p.getType().toString().replace("IDENTITY", "").replace("identity", "").replace("NULL", "")
								.replace("NOT", "").replace("null", "").replace("not", "").replace("Null", "")
								.replace("Not", "").trim().toString()
								.equals(c.getType().toString().replace("IDENTITY", "").replace("identity", "")
										.replace("NULL", "").replace("NOT", "").replace("null", "").replace("not", "")
										.replace("Null", "").replace("Not", "").trim()))
							continue;

						while (threadCounter == MAX_THREAD) {
							Thread.sleep(1000);
						}
						threadCounter++;
						new Thread(new Runnable() {
							public void run() {
								System.out.println(Thread.currentThread().getName());
								writeToFile(runQuery(primarytable.getName(), p.getName(), comparingtable.getName(),
										c.getName(), connection));
								threadCounter--;
							}
						}).start();
					}
				}

			}
		}

		while (threadCounter != 0) {
			Thread.sleep(10);
		}
		Thread.sleep(200);
		out.flush();
		out.close();

		System.out.println("Analysis Completed " + new Date());
	}

	private static void writeToFile(String line) {
		if(line.equals(""))
			return;
		try {
			out.write(line);
			out.write("");
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static boolean isBlob(int columnDataType) {
		if (columnDataType == Types.BLOB || columnDataType == Types.VARBINARY || columnDataType == Types.NVARCHAR
				|| columnDataType == Types.LONGNVARCHAR || columnDataType == Types.BINARY || columnDataType == Types.BIT
				|| columnDataType == Types.BOOLEAN || columnDataType == Types.CLOB || columnDataType == Types.NCLOB)
			return true;
		return false;
	}

	static final String sqlQuery1 = "select count(*) from (select distinct top noOfRecords primaryColumn from primaryTable) as internalQuery";
	static final String sqlQuery2 = "select count(*) from (select distinct b.secondaryColumn from primaryTable a,  secondaryTable b where a.primaryColumn = b.secondaryColumn and a.primaryColumn in (select distinct top noOfRecords primaryColumn from primaryTable)) as internalQuery";

	public static String getURL() {
		switch (SERVER.toLowerCase()) {
		case "sql":
			return "jdbc:sqlserver://" + HOST + ":" + PORT + ";database=" + DB;
		case "sqlwinauth":
			return "jdbc:sqlserver://" + HOST + ":" + PORT + ";databaseName=" + DB + ";integratedSecurity=true";
		case "oracle":
			return "jdbc:oracle:thin:@" + HOST + ":" + PORT + ":" + DB;
		case "oracleservice":
			return "jdbc:oracle:thin:@//" + HOST + ":" + PORT + "/" + DB;
		case "mysql":
			return "jdbc:mysql://" + HOST + ":" + PORT + "/" + DB
					+ "?nullNamePatternMatchesAll=true&logger=Jdk14Logger&dumpQueriesOnException=true&dumpMetadataOnColumnNotFound=true&maxQuerySizeToLog=4096&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone="
					+ CommonFunctions.getTimeZone() + "&disableMariaDbDriver";
		case "mariadb":
		case "maria":
			return "jdbc:mariadb://" + HOST + ":" + PORT + "/" + DB;
		case "db2":
			return "jdbc:db2://" + HOST + ":" + PORT + "/" + DB + ";retrieveMessagesFromServerOnGetMessage=true;";
		case "postgresql":
			return "jdbc:postgresql://" + HOST + ":" + PORT + "/" + DB;
		case "sybase":
			return "jdbc:jtds:sybase://" + HOST + ":" + PORT + "/" + DB;
		}
		return null;
	}

	public static String getSchemaSelection() {
		switch (SERVER.toLowerCase()) {
		case "sql":
		case "sybase":
			return DB + "." + SCHEMA;
		case "oracle":
		case "oracleservice":
		case "mysql":
		case "db2":
		case "postgresql":
			return SCHEMA;
		}
		return null;
	}

	public static String runQuery(String primaryTable, String primaryColumn, String secondaryTable,
			String secondaryColumn, Connection con) {
		int countQuery1 = 0;
		int countQuery2 = 0;
		int countQuery3 = 0;
		int countQuery4 = 0;
		int a = 0;
		int b = 0;
		Statement st = null;
		ResultSet rs1 = null;
		ResultSet rs2 = null;
		ResultSet rs3 = null;
		ResultSet rs4 = null;

		try {
			st = con.createStatement();
			rs1 = st.executeQuery(sqlQuery1.replace("primaryTable", primaryTable)
					.replace("primaryColumn", primaryColumn).replace("noOfRecords", recordCount));
			while (rs1.next()) {
				countQuery1 = rs1.getInt(1);
			}

			rs2 = st.executeQuery(sqlQuery2.replace("primaryTable", primaryTable)
					.replace("primaryColumn", primaryColumn).replace("secondaryTable", secondaryTable)
					.replaceAll("secondaryColumn", secondaryColumn).replace("noOfRecords", recordCount));
			while (rs2.next()) {
				countQuery2 = rs2.getInt(1);
			}

			a = (int) Math.round((double) countQuery2 * 100 / countQuery1);
			if (a != 100) {
				rs3 = st.executeQuery(sqlQuery1.replace("primaryTable", secondaryTable)
						.replace("primaryColumn", secondaryColumn).replace("noOfRecords", recordCount));
				while (rs3.next()) {
					countQuery3 = rs3.getInt(1);
				}

				rs4 = st.executeQuery(sqlQuery2.replace("secondaryTable", primaryTable)
						.replace("secondaryColumn", primaryColumn).replace("primaryTable", secondaryTable)
						.replaceAll("primaryColumn", secondaryColumn).replace("noOfRecords", recordCount));
				while (rs4.next()) {
					countQuery4 = rs4.getInt(1);
				}

				b = (int) Math.round((double) countQuery4 * 100 / countQuery3);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs1 != null)
					rs1.close();
				if (rs2 != null)
					rs2.close();
				if (rs3 != null)
					rs3.close();
				if (rs4 != null)
					rs4.close();
				if (st != null)
					st.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		/*if(a == 0 && b ==0)
			return "";*/
		if ((a >= b))
			return (primaryTable + "," + primaryColumn + "," + secondaryTable + "," + secondaryColumn + "," + a + "\n");
		else
			return (secondaryTable + "," + secondaryColumn + "," + primaryTable + "," + primaryColumn + "," + b + "\n");
	}

}
