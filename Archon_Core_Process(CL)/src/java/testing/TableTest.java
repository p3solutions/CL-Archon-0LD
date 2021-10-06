package testing;

import static us.fatehi.commandlineparser.CommandLineUtility.applyApplicationLogLevel;
import static us.fatehi.commandlineparser.CommandLineUtility.logSystemProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;

import javax.sql.DataSource;

import schemacrawler.schema.Catalog;
import schemacrawler.schema.Column;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;
import schemacrawler.schema.View;
import schemacrawler.schemacrawler.DatabaseConnectionOptions;
import schemacrawler.schemacrawler.ExcludeAll;
import schemacrawler.schemacrawler.IncludeAll;
import schemacrawler.schemacrawler.RegularExpressionInclusionRule;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaInfoLevelBuilder;
import schemacrawler.tools.text.operation.OperationExecutable;
import schemacrawler.tools.text.operation.OperationOptions;
import schemacrawler.utility.SchemaCrawlerUtility;

public class TableTest {
	static Map<String, Integer> tableHirachy = new LinkedHashMap<String, Integer>();
	static Map<String, LinkedHashMap<String, Boolean>> tableExt = new LinkedHashMap<String, LinkedHashMap<String, Boolean>>();

	public static void main(String[] args) throws Exception {
//		args = new String[] {
//				"jdbc:mysql://localhost:3306/CLAIMS_SYS?nullNamePatternMatchesAll=true&logger=Jdk14Logger&dumpQueriesOnException=true&dumpMetadataOnColumnNotFound=true&maxQuerySizeToLog=4096&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&disableMariaDbDriver",
//				"CLAIMS_SYS", "root", "secret" };
		applyApplicationLogLevel(Level.OFF);
		logSystemProperties();

		final SchemaCrawlerOptions options = new SchemaCrawlerOptions();
		options.setSchemaInclusionRule(new RegularExpressionInclusionRule(args[1]));
		options.setSchemaInfoLevel(SchemaInfoLevelBuilder.standard());
		options.setRoutineInclusionRule(new ExcludeAll());
		options.setColumnInclusionRule(new IncludeAll());
		options.setRoutineColumnInclusionRule(new ExcludeAll());
		options.setRoutineInclusionRule(new ExcludeAll());

		//options.setTableInclusionRule(new RegularExpressionInclusionRule(args[1] + "." + ".ACTOR\\$."));

		final Catalog catalog = SchemaCrawlerUtility.getCatalog(getConnection(args), options);

		for (final Schema schema : catalog.getSchemas()) {
			System.out.println(schema);
			for (final Table table : catalog.getTables(schema)) {
				System.out.print("o--> " + table);
				if (table instanceof View) {
					System.out.println(" (VIEW)");
				} else {
					System.out.println();
				}
				System.out.println("--------------------");
				for (Column col : table.getColumns()) {
					System.out.println(col.getFullName());
				}
				System.out.println("--------------------");
				System.out.println("\n\n\n");
			}
		}

		System.out.println();
		System.out.println();
		System.out.println("Row Count");
		System.out.println("---------");
		System.out.println();
		OperationExecutable count = new OperationExecutable("count");
		OperationOptions operationOptions = new OperationOptions();
		count.setSchemaCrawlerOptions(options);
		count.setOperationOptions(operationOptions);
		final Catalog countCatalog = SchemaCrawlerUtility.getCatalog(getConnection(args), options);
		count.executeOn(countCatalog, getConnection(args));
		System.out.println("Row Count Completed");
	}

	private static Connection getConnection(String args[]) throws SchemaCrawlerException, SQLException {
		final DataSource dataSource = new DatabaseConnectionOptions(args[0]);
		return dataSource.getConnection(args[2], args[3]);
	}

}
