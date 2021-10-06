package testing;

import static us.fatehi.commandlineparser.CommandLineUtility.applyApplicationLogLevel;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import javax.sql.DataSource;

import com.p3.archon.commonfunctions.CommonFunctions;
import com.p3.archon.coreprocess.common.Constants;
import com.p3.archon.coreprocess.validations.Validations;

import schemacrawler.schema.Catalog;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.Config;
import schemacrawler.schemacrawler.DatabaseConnectionOptions;
import schemacrawler.schemacrawler.ExcludeAll;
import schemacrawler.schemacrawler.RegularExpressionInclusionRule;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.tools.integration.graph.GraphExecutable;
import schemacrawler.tools.integration.graph.GraphOptions;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.tools.text.operation.OperationExecutable;
import schemacrawler.tools.text.operation.OperationOptions;
import schemacrawler.tools.text.schema.SchemaTextExecutable;
import schemacrawler.tools.text.schema.SchemaTextOptions;
import schemacrawler.utility.SchemaCrawlerUtility;

public class TestingMain {
	static Map<String, Integer> tableHirachy = new LinkedHashMap<String, Integer>();
	static Map<String, LinkedHashMap<String, Boolean>> tableExt = new LinkedHashMap<String, LinkedHashMap<String, Boolean>>();
	static int max = 0;

	static String SERVER = "SQL";
	static String HOST = "localhost";
	static String PORT = "1433";
	static String DB = "CLAIMS_SYS";
	static String SCHEMA = "dbo";
	static String USER = "admin";
	static String PASS = "secret";

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
					+ "?nullNamePatternMatchesAll=true&logger=Jdk14Logger&dumpQueriesOnException=true&dumpMetadataOnColumnNotFound=true&maxQuerySizeToLog=4096&&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone="
					+ CommonFunctions.getTimeZone() + "&disableMariaDbDriver";
		case "maria":
		case "mariadb":
			return "jdbc:mariadb://" + HOST + ":" + PORT + "/" + DB;
		case "db2":
			return "jdbc:db2://" + HOST + ":" + PORT + "/" + DB + ";retrieveMessagesFromServerOnGetMessage=true;";
		case "postgresql":
			return "jdbc:postgresql://" + HOST + ":" + PORT + "/" + DB;
		case "sybase":
			return "jdbc:jtds:sybase://" + HOST + ":" + PORT + "/" + DB;
		case "cache":
			try {
				Class.forName("com.intersys.jdbc.CacheDriver").newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return "jdbc:Cache://" + HOST + ":" + PORT + "/" + DB;

		case "interbase":
			try {
				Class.forName("interbase.interclient.Driver").newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return "jdbc:interbase://" + HOST + ":" + PORT + "/" + DB;

		}
		return null;
	}

	public static String getSchemaSelection() {
		switch (SERVER.toLowerCase()) {
		case "sql":
		case "sqlwinauth":
		case "sybase":
			return DB + "." + SCHEMA;
		case "oracle":
		case "oracleservice":
		case "mysql":
		case "maria":
		case "mariadb":
		case "db2":
		case "postgresql":
		case "cache":
			return SCHEMA;
		}
		return null;
	}

	public static void main(final String[] args) throws Exception {

		applyApplicationLogLevel(Level.OFF);

		final DataSource dataSource = new DatabaseConnectionOptions(getURL());
		final Connection connection = dataSource.getConnection(USER, PASS);
		final SchemaCrawlerOptions options = new SchemaCrawlerOptions();
		options.setRoutineInclusionRule(new ExcludeAll());
		options.setSchemaInclusionRule(new RegularExpressionInclusionRule(getSchemaSelection()));
		options.setTableInclusionRule(new RegularExpressionInclusionRule("*"));

		Collection<String> tableTypes = new ArrayList<String>();
		tableTypes.add("Table");
		tableTypes.add("View");
		options.setTableTypes(tableTypes);

		final Catalog catalog = SchemaCrawlerUtility.getCatalog(connection, options);
		final List<Table> allTables = new ArrayList<>(catalog.getTables());

		// Schema
		System.out.println("Schema");
		System.out.println("------");
		System.out.println();

		SchemaTextExecutable schemaTextExe = new SchemaTextExecutable("schema");
		SchemaTextOptions schemaTextOptions = new SchemaTextOptions();
		schemaTextOptions.setShowOrdinalNumbers(true);
		schemaTextOptions.setShowStandardColumnTypeNames(true);
		schemaTextOptions.setShowUnqualifiedNames(true);
		schemaTextOptions.setNoInfo(true);
		schemaTextOptions.setAlphabeticalSortForForeignKeys(true);
		schemaTextOptions.setAlphabeticalSortForIndexes(true);
		schemaTextOptions.setAlphabeticalSortForRoutineColumns(true);
		schemaTextOptions.setAlphabeticalSortForTables(true);
		OutputOptions schemaOutputOptions = new OutputOptions();
		schemaOutputOptions.setOutputFormatValue("csv");
		new File("Output").mkdirs();
		Path path = Paths.get("Output" + File.separator + "schema.csv");
		schemaOutputOptions.setOutputFile(path);
		schemaTextExe.setOutputOptions(schemaOutputOptions);
		schemaTextExe.setSchemaTextOptions(schemaTextOptions);
		final Catalog schemaCatalog = SchemaCrawlerUtility.getCatalog(connection, options);
		schemaTextExe.executeOn(schemaCatalog, connection);
		System.out.println("Schema generation Completed");

//		// Rowcount
		System.out.println();
		System.out.println();
		System.out.println("Row Count");
		System.out.println("---------");
		System.out.println();
		OperationExecutable count = new OperationExecutable("count");
		OperationOptions operationOptions = new OperationOptions();
		count.setSchemaCrawlerOptions(options);
		count.setOperationOptions(operationOptions);
		final Catalog countCatalog = SchemaCrawlerUtility.getCatalog(connection, options);
		count.executeOn(countCatalog, connection);
		System.out.println("Row Count Completed");

//		// Graph
		System.out.println();
		System.out.println();
		System.out.println("Graph");
		System.out.println("---------------");
		System.out.println();
		GraphExecutable graph = new GraphExecutable("graph");
		GraphOptions op = new GraphOptions();
		op.setDotPath("dot" + File.separator + "dot");
		op.setNoHeader(true);
		op.setNoInfo(true);
		op.setNoFooter(true);
		op.setShowOrdinalNumbers(true);
		new File("Output").mkdirs();
		OutputOptions outputOptions = new OutputOptions();
		outputOptions.setOutputFormatValue("pdf");
		outputOptions.setOutputFile(Paths.get("Output" + File.separator + "test.pdf"));
		graph.setOutputOptions(outputOptions);
		graph.setGraphOptions(op);
		final Catalog graphCatalog = SchemaCrawlerUtility.getCatalog(connection, options);
		graph.executeOn(graphCatalog, connection);
		System.out.println("Graph generation Completed");

		// dump
		System.out.println();
		System.out.println();
		System.out.println("Data Processing");
		System.out.println("---------------");
		System.out.println();

		Map<String, ArrayList<Date>> statistics = new LinkedHashMap<String, ArrayList<Date>>();

		for (final Table table : allTables) {
			
			
			System.out.println("Processing Table " + table.getName());
			ArrayList<Date> dataGatherer = new ArrayList<Date>();
			dataGatherer.add(new Date());
			final SchemaCrawlerOptions tableDumpOptions = new SchemaCrawlerOptions();
			tableDumpOptions.setRoutineInclusionRule(new ExcludeAll());
			tableDumpOptions.setTableInclusionRule(new RegularExpressionInclusionRule(table.getFullName()));

			OperationExecutable dump = new OperationExecutable("quickdump");
			OperationOptions dumpOpOptions = new OperationOptions();
			dumpOpOptions.setShowUnqualifiedNames(true);
			dumpOpOptions.setXmlChunkLimit(500000);
			dumpOpOptions.setShowLobs(true);
			dump.setSchemaCrawlerOptions(tableDumpOptions);

			OutputOptions dumpOptions = new OutputOptions();
			dumpOptions.setOutputFormatValue("xml");
			new File("Output").mkdirs();
			Path dumppath = Paths.get("Output" + File.separator + Validations.checkValidFile(table.getName()) + ".xml");
			dumpOptions.setOutputFile(dumppath);

			dump.setOutputOptions(dumpOptions);
			dump.setOperationOptions(dumpOpOptions);

			final Catalog dumpCatalog = SchemaCrawlerUtility.getCatalog(connection, tableDumpOptions);
			dump.executeOn(dumpCatalog, connection);

			splitFile(dumppath);
			dataGatherer.add(new Date());
			statistics.put(table.getName(), dataGatherer);
		}
		System.out.println("Dump Job Completed");

		// DataPull
		System.out.println();
		System.out.println();
		System.out.println("DataPull");
		System.out.println("---------------");
		System.out.println();

		for (final Table table : allTables) {

			OperationExecutable pullDataOperationExe = new OperationExecutable(
					Validations.checkValidFile(table.getName()));
			OperationOptions pullOpOptions = new OperationOptions();
			Config c = new Config();
			c.put(Validations.checkValidFile(table.getName()), "Select * from " + table.getFullName());
			pullDataOperationExe.setAdditionalConfiguration(c);
			pullOpOptions.setShowUnqualifiedNames(true);
			pullDataOperationExe.setSchemaCrawlerOptions(options);

			OutputOptions pullOutputOptions = new OutputOptions();
			pullOutputOptions.setOutputFormatValue("html");
			new File("Output").mkdirs();
			Path dppath = Paths.get(
					"Output" + File.separator + "Testing_" + Validations.checkValidFile(table.getName()) + ".html");
			pullOutputOptions.setOutputFile(dppath);

			pullDataOperationExe.setOutputOptions(pullOutputOptions);
			pullDataOperationExe.setOperationOptions(pullOpOptions);
			final Catalog pullCatalog = SchemaCrawlerUtility.getCatalog(connection, options);
			pullDataOperationExe.executeOn(pullCatalog, connection);
		}

		System.out.println("Data pull Job Completed");

		// statistics
		System.out.println();
		System.out.println();
		System.out.println("Statistics");
		System.out.println("----------");
		System.out.println();
		viewFormatter(prepareObjectArray(4));
		viewFormatter(new Object[] { "TABLE", "START TIME", "END TIME", "PROCESSING TIME (+/- 1 sec)" });
		viewFormatter(prepareObjectArray(4));
		for (String stat : statistics.keySet()) {
			viewFormatter(new Object[] { stat, statistics.get(stat).get(0), statistics.get(stat).get(1),
					((statistics.get(stat).get(1).getTime() - statistics.get(stat).get(0).getTime()) / 1000)
							+ "seconds" });
		}
		viewFormatter(prepareObjectArray(4));
	}

	private static void splitFile(Path path) {
		File f = new File(path.toString());
		int i = 1;
		File w = new File(path.toString().substring(0, path.toString().indexOf(".xml")) + "_chunk_" + i++ + ".xml");
		try {
			FileReader reader = new FileReader(f);
			FileWriter writer = new FileWriter(w);
			BufferedReader bufferedReader = new BufferedReader(reader);
			BufferedWriter bufferedWriter = new BufferedWriter(writer);
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				if (line.equals("&lt;!-- ----- SPLIT HERE ----- --&gt;")) {
					bufferedWriter.flush();
					bufferedWriter.close();
					w = new File(
							path.toString().substring(0, path.toString().indexOf(".xml")) + "_chunk_" + i++ + ".xml");
					writer = new FileWriter(w);
					bufferedWriter = new BufferedWriter(writer);
				} else {
					bufferedWriter.write(line);
					bufferedWriter.write("\n");
				}
			}
			bufferedWriter.flush();
			bufferedWriter.close();
			bufferedReader.close();
			path.toFile().delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Object[] prepareObjectArray(int i) {
		Object[] array = new Object[i];
		for (int j = 0; j < array.length; j++) {
			array[j] = "----------------------------------------";
		}
		return array;
	}

	private static void viewFormatter(Object[] strings) {
		for (int i = 0; i < strings.length; i++) {
			System.out.print(rightPadding(strings[i] + "", 40));
		}
		System.out.println();
	}

	public static String rightPadding(String str, int num) {
		return String.format("%1$-" + num + "s", str);
	}
}
