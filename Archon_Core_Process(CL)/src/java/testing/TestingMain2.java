package testing;

import static us.fatehi.commandlineparser.CommandLineUtility.applyApplicationLogLevel;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.UUID;
import java.util.logging.Level;

import javax.sql.DataSource;

import schemacrawler.schema.Catalog;
import schemacrawler.schemacrawler.Config;
import schemacrawler.schemacrawler.DatabaseConnectionOptions;
import schemacrawler.schemacrawler.ExcludeAll;
import schemacrawler.schemacrawler.IncludeAll;
import schemacrawler.schemacrawler.RegularExpressionInclusionRule;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaInfoLevelBuilder;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.tools.text.operation.OperationExecutable;
import schemacrawler.tools.text.operation.OperationOptions;
import schemacrawler.utility.SchemaCrawlerUtility;

public class TestingMain2 {
	public static void main(final String[] args) throws Exception {

		boolean fileNameExtOption  = false;		// can be "" or "COLUMN"
		String fileNameOption  = "COLUMN"; 		// can be "" or "COLUMN" or "SEQNAME"
		
		int fileNameStartSeq = 100001;			// start number for SEQ
		String fileNamePrefixSeq = "ABDGLJA";	// Prefix name for Extraction File (used if SEQNAME is used
		String fileNameColumn = "FILE_NAME";	// Column name to refer file name for BLOBS
		
		String fileNameExtColumn = "FILE_NAME";	// Column name to refer file extesion for BLOBS
		String fileNameExtValue = "";			// file extesion for BLOBS
		
		String blobColumn = "CONTENT";			// Column name to refer for BLOBS
		String tableName = "UNSTRUCTURED";
		
		String saveloc = "C:\\Users\\Malik\\Desktop\\A";
		
		applyApplicationLogLevel(Level.OFF);
		
		// SQL
		final DataSource dataSource = new DatabaseConnectionOptions(
				"jdbc:sqlserver://localhost:1433;database=CLAIMS_SYS");
		final Connection connection = dataSource.getConnection("admin", "secret");
		
		final SchemaCrawlerOptions options = new SchemaCrawlerOptions();
		options.setSchemaInclusionRule(new RegularExpressionInclusionRule("CLAIMS_SYS.dbo"));
		options.setSchemaInfoLevel(SchemaInfoLevelBuilder.standard());
	    options.setRoutineInclusionRule(new ExcludeAll());
	    options.setColumnInclusionRule(new IncludeAll());
	    options.setRoutineColumnInclusionRule(new ExcludeAll());
	    options.setRoutineInclusionRule(new ExcludeAll());
	    
	    OperationOptions operationOptions = new OperationOptions();
		
		operationOptions.setShowUnqualifiedNames(true);
		operationOptions.setShowLobs(true);
		
		switch(fileNameOption.toUpperCase()){
		case "SEQNAME" :
			operationOptions.setFilenameOverride(true);
			operationOptions.setFilenameOverrideValue(fileNamePrefixSeq);
			operationOptions.setSeqStartValue(fileNameStartSeq);
			break;
		case "COLUMN" :
			operationOptions.setFilename(true);
			operationOptions.setFilenameColumn("\"" + fileNameColumn + "\"");
			operationOptions.setSeqStartValue(1);
			break;
		default :
			operationOptions.setSeqFilename(true);
			operationOptions.setSeqStartValue(fileNameStartSeq);
			break;
		}
		
		OperationExecutable opterationExe = new OperationExecutable("file");
	    Config c = new Config();
	    StringBuffer sb = new StringBuffer();
	    sb.append("select ");
	    if(fileNameOption.toUpperCase().equalsIgnoreCase("COLUMN"))
	    	sb.append(operationOptions.getFilenameColumn()).append(" ,");
	    if (fileNameExtOption)
			sb.append("\"" + fileNameExtColumn + "\"").append(" ,");
		else
			sb.append("'").append(fileNameExtValue).append("' ,");
	    sb.append("\"" + blobColumn + "\"");
	    sb.append(" from ").append(tableName);
		c.put("file", sb.toString());
		opterationExe.setAdditionalConfiguration(c);
		
		opterationExe.setSchemaCrawlerOptions(options);
		OutputOptions outputOptions = new OutputOptions();
		outputOptions.setOutputFormatValue("file");
		new File(saveloc).mkdirs();
		Path path = Paths.get(saveloc + File.separator + "log_" + UUID.randomUUID() +".txt");
		outputOptions.setOutputFile(path);
		operationOptions.setAppendOutput(true);
		opterationExe.setOutputOptions(outputOptions);
		opterationExe.setOperationOptions(operationOptions);
		
		final Catalog tableDumpCatalog = SchemaCrawlerUtility.getCatalog(connection, options);
		opterationExe.executeOn(tableDumpCatalog, connection);
		
	}
}
	
