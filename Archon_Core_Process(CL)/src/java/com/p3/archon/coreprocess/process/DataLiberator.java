package com.p3.archon.coreprocess.process;

import static us.fatehi.commandlineparser.CommandLineUtility.applyApplicationLogLevel;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Date;

import javax.sql.DataSource;

import com.p3.archon.commonfunctions.CommonFunctions;
import com.p3.archon.coreprocess.beans.ArchonInputBean;
import com.p3.archon.coreprocess.common.Constants;
import com.p3.archon.coreprocess.executables.DbInfoExecutionJob;
import com.p3.archon.coreprocess.executables.DbLinkQueryExecutionJob;
import com.p3.archon.coreprocess.executables.DumpMainExecutionJob;
import com.p3.archon.coreprocess.executables.ExecutionJob;
import com.p3.archon.coreprocess.executables.GraphExecutionJob;
import com.p3.archon.coreprocess.executables.MetadataExecutionJob;
import com.p3.archon.coreprocess.executables.QueryExecutionJob;
import com.p3.archon.coreprocess.executables.RowCountExecutionJob;
import com.p3.archon.coreprocess.executables.SchemaExecutionJob;
import com.p3.archon.coreprocess.executables.TestExecutionJob;
import com.p3.archon.coreprocess.executables.UnstructuredExecutionJob;

import schemacrawler.schemacrawler.DatabaseConnectionOptions;
import schemacrawler.schemacrawler.SchemaCrawlerException;

public class DataLiberator {

	private ArchonInputBean inputArgs;

	public DataLiberator(ArchonInputBean inputArgs) {
		this.inputArgs = inputArgs;
	}

	public void testConnection() throws SQLException, SchemaCrawlerException {
		applyApplicationLogLevel(Constants.LOG_LEVEL);
		final DataSource dataSource = new DatabaseConnectionOptions(getConnectionString(inputArgs.getDatabaseServer()));
		final Connection connection = dataSource.getConnection(inputArgs.getUser(), inputArgs.getPass());
		connection.close();
	}

	public void start() throws Exception {
		applyApplicationLogLevel(Constants.LOG_LEVEL);
		final DataSource dataSource = new DatabaseConnectionOptions(getConnectionString(inputArgs.getDatabaseServer()));
		final Connection connection = dataSource.getConnection(inputArgs.getUser(), inputArgs.getPass());
		perfromActivity(dataSource, connection);
	}

	private void perfromActivity(DataSource dataSource, Connection connection) throws Exception {
		ExecutionJob exe;
		// inputArgs.printInputs();
		Date startTime = new Date();
		System.out.println("--------------------------------- Operation start ---------------------------------");
		switch (inputArgs.getCommand().trim().toLowerCase()) {
		case "dblinkquery":
			exe = new DbLinkQueryExecutionJob(dataSource, connection, inputArgs);
			exe.start();
			break;
		case "dbinfo":
			exe = new DbInfoExecutionJob(dataSource, connection, inputArgs);
			exe.start();
			break;
		case "count":
			exe = new RowCountExecutionJob(dataSource, connection, inputArgs);
			exe.start();
			break;
		case "schemageneration":
		case "schema":
			exe = new SchemaExecutionJob(dataSource, connection, inputArgs);
			exe.start();
			break;
		case "graph":
			String[] sl = (inputArgs.getSchema().startsWith("(") && inputArgs.getSchema().endsWith(")"))
					? inputArgs.getSchema().substring(1, inputArgs.getSchema().length() - 1).split("\\|")
					: inputArgs.getSchema().split("\\|");
			for (String schema : sl) {
				inputArgs.setSchema(schema);
				exe = new GraphExecutionJob(dataSource, connection, inputArgs);
				exe.start();
			}
			break;
		case "metadata":

			String[] sl1 = (inputArgs.getSchema().startsWith("(") && inputArgs.getSchema().endsWith(")"))
					? inputArgs.getSchema().substring(1, inputArgs.getSchema().length() - 1).split("\\|")
					: inputArgs.getSchema().split("\\|");
			for (String schema : sl1) {
				inputArgs.setSchema(schema);
				if (inputArgs.outputFormat.equals("xml"))
					exe = new MetadataExecutionJob(dataSource, connection, inputArgs);
				else
					exe = new SchemaExecutionJob(dataSource, connection, inputArgs);
				exe.start();
			}

			break;
		case "dataextract":
		case "quickdump":
			exe = new DumpMainExecutionJob(dataSource, connection, inputArgs);
			exe.start();
			break;
		case "query":
			exe = new QueryExecutionJob(dataSource, connection, inputArgs);
			exe.start();
			break;
		case "test":
			exe = new TestExecutionJob(dataSource, connection, inputArgs);
			exe.start();
			break;
		case "unstructured":
			try {
				exe = new UnstructuredExecutionJob(dataSource, connection, inputArgs);
			} catch (Exception e) {
				System.out.println("ERROR :");
				System.out.println(e.getMessage());
				throw e;
			}
			exe.start();
			break;
		default:
			return;
		}
		Date endTime = new Date();
		System.out.println("Job Execution Time : " + ExecutionJob.timeDiff(endTime.getTime() - startTime.getTime()));

		System.out.println("------------------------------- Operation Compeleted -------------------------------");
	}

	private String getConnectionString(String type) {
		Object[] replaceValues = new Object[] { inputArgs.getHost(), inputArgs.getPort(), inputArgs.getDatabase(),
				inputArgs.userName, inputArgs.password };
		return MessageFormat.format(getConnectionURL(type), replaceValues);
	}

	private String getConnectionURL(String type) {
		switch (type.toLowerCase()) {
		case "teradata":
			return "jdbc:teradata://{0}/{2}";
		case "sqlwinauth":
			return "jdbc:sqlserver://{0}:{1};databaseName={2};integratedSecurity=true";
//			return "jdbc:jtds:sqlserver://localhost/{2};appName=SchemaCrawler;useCursors=true;useNTLMv2=true;domain={0};";
		case "sql":
			return "jdbc:sqlserver://{0}:{1};database={2}";
		case "oracle":
			return "jdbc:oracle:thin:@{0}:{1}:{2}";
		case "oracleservice":
			return "jdbc:oracle:thin:@//{0}:{1}/{2}";
		case "mysql":
			return "jdbc:mysql://{0}:{1}/{2}?nullNamePatternMatchesAll=true&logger=Jdk14Logger&dumpQueriesOnException=true&dumpMetadataOnColumnNotFound=true&maxQuerySizeToLog=4096&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=true&serverTimezone="
					+ CommonFunctions.getTimeZone() + "&disableMariaDbDriver";
		case "mariadb":
		case "maria":
			return "jdbc:mariadb://{0}:{1}/{2}";
		case "db2":
			return "jdbc:db2://{0}:{1}/{2};retrieveMessagesFromServerOnGetMessage=true;";
		case "sybase":
			return "jdbc:jtds:sybase://{0}:{1}/{2}";
		case "postgresql":
			return "jdbc:postgresql://{0}:{1}/{2}?ApplicationName=SchemaCrawler";
		case "as400":
			return "jdbc:as400://{0}:{1}/{2}";
		case "as400noport":
			return "jdbc:as400://{0}/{2}";
		default:
			return null;
		}
	}
}
