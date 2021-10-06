package malik.testing;

import static us.fatehi.commandlineparser.CommandLineUtility.applyApplicationLogLevel;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;

import javax.sql.DataSource;

import schemacrawler.schema.Catalog;
import schemacrawler.schema.Column;
import schemacrawler.schema.Table;
import schemacrawler.schema.View;
import schemacrawler.schemacrawler.DatabaseConnectionOptions;
import schemacrawler.schemacrawler.ExcludeAll;
import schemacrawler.schemacrawler.RegularExpressionInclusionRule;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.utility.SchemaCrawlerUtility;

public class TryColumnMatch {
	
	private static Map<Integer,String> tableKeyRef = new TreeMap<Integer,String>();
	private static Map<String,TreeSet<Integer>> columnAvailabeTableRef = new TreeMap<String,TreeSet<Integer>>();
	private static Map<String,TreeMap<String,TreeSet<String>>> srcDestCol = new TreeMap<String,TreeMap<String,TreeSet<String>>>();
	
	private static Stack<String> stack = new Stack<String>();
	private static Map<String, ArrayList<Stack<String>>> map = new TreeMap<String,ArrayList<Stack<String>>>();
	
	private static String tableName = "CLAIMS_SYS.dbo.SUBSCRIBER";
	public static void main(final String[] args) throws Exception {

		applyApplicationLogLevel(Level.OFF);
		
/*		
		// Mysql
		final DataSource dataSource = new DatabaseConnectionOptions("jdbc:mysql://localhost:3306/sakila?nullNamePatternMatchesAll=true&logger=Jdk14Logger&dumpQueriesOnException=true&dumpMetadataOnColumnNotFound=true&maxQuerySizeToLog=4096&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&disableMariaDbDriver");
		final Connection connection = dataSource.getConnection("admin", "secret");
		final SchemaCrawlerOptions options = new SchemaCrawlerOptions();
		options.setRoutineInclusionRule(new ExcludeAll());
		options.setSchemaInclusionRule(new RegularExpressionInclusionRule("sakila.*"));


		// Oracle
		final DataSource dataSource = new DatabaseConnectionOptions("jdbc:oracle:thin:@localhost:1521:orcl1");
		final Connection connection = dataSource.getConnection("sh", "orcl1");
		final SchemaCrawlerOptions options = new SchemaCrawlerOptions();
		options.setRoutineInclusionRule(new ExcludeAll());
		options.setSchemaInclusionRule(new RegularExpressionInclusionRule("SH.*"));
*/

		// SQL
		final DataSource dataSource = new DatabaseConnectionOptions(
				"jdbc:sqlserver://localhost:1433;database=CLAIMS_SYS");
		final Connection connection = dataSource.getConnection("admin", "secret");
		final SchemaCrawlerOptions options = new SchemaCrawlerOptions();
		options.setRoutineInclusionRule(new ExcludeAll());
		options.setSchemaInclusionRule(new RegularExpressionInclusionRule("CLAIMS_SYS.dbo.*"));

		
		final Catalog catalog = SchemaCrawlerUtility.getCatalog(connection, options);
		final List<Table> allTables = new ArrayList<>(catalog.getTables());
		
		int tableCounter = 0;
		for(Table table : allTables){
			if(table instanceof View)
				continue;
			tableKeyRef.put(tableCounter, table.getFullName());
			for(Column column : table.getColumns()){
				String columnKeyName = column.getName() + column.getType() + column.getSize();
				TreeSet<Integer> temp = columnAvailabeTableRef.get(columnKeyName);
				if(temp == null)
					temp = new TreeSet<Integer>();
				temp.add(tableCounter);
				columnAvailabeTableRef.put(columnKeyName, temp);
			}
			tableCounter++;
		}
		
		
		
		for(Table table : allTables){
			if(table instanceof View)
				continue;
			System.out.println(table.getFullName());
			for(Column column : table.getColumns()){
				// if not a part of primary key skip
				if(!column.isPartOfPrimaryKey())
					continue;
				/*System.out.println("\t" + column.getName());
				
				String columnKeyName = column.getName() + column.getType() + column.getSize();
				for(int i : columnAvailabeTableRef.get(columnKeyName)){
					if(!tableKeyRef.get(i).equals(table.getFullName()))
						System.out.println("\t\t" + tableKeyRef.get(i));
				}*/
				
				TreeMap<String,TreeSet<String>> temp = srcDestCol.get(table.getFullName());
				if(temp == null)
					temp = new TreeMap<String,TreeSet<String>>();
				
				String columnKeyName = column.getName() + column.getType() + column.getSize();
				for(int i : columnAvailabeTableRef.get(columnKeyName)){
					if(!tableKeyRef.get(i).equals(table.getFullName())){
						TreeSet<String> tempSet = temp.get(tableKeyRef.get(i));
						if(tempSet == null)
							tempSet = new TreeSet<String>();
						
						TreeMap<String,TreeSet<String>> revtemp = srcDestCol.get(tableKeyRef.get(i));
						if(revtemp == null)
							revtemp = new TreeMap<String,TreeSet<String>>();
						
						TreeSet<String> revtempSet = revtemp.get(table.getFullName());
						if(revtempSet == null)
							revtempSet = new TreeSet<String>();
						
						tempSet.add(column.getName());
						
						temp.put(tableKeyRef.get(i), tempSet);
						revtemp.put(table.getFullName(), tempSet);
						srcDestCol.put(tableKeyRef.get(i), revtemp);	
					}
				}
				srcDestCol.put(table.getFullName(), temp);
			}
			//System.out.println("-------------------------");
		}
		
		System.out.println(srcDestCol);
		
		for (String src : srcDestCol.keySet()) {
			System.out.println(src);
			for (String dest : srcDestCol.get(src).keySet()) {
				System.out.println("\t" + dest);
				for (String join : srcDestCol.get(src).get(dest)) {
					System.out.println("\t\t" + join);
				}
			}
		}
		
		stackImp(tableName, 0);
		processLogic(tableName,0);
		
		System.out.println("---------------STACK MAP-----------------");
		for (String string : map.keySet()) {
			System.out.println(string);
			System.out.println("------------------");
			for (Stack<String> string1 : map.get(string)) {
				System.out.println(string1);
			}
			System.out.println();
			System.out.println();
		}
		
		
	
	 	for(int i : tableKeyRef.keySet())
			System.out.println(i + " " + tableKeyRef.get(i));
		System.out.println();		
		for(String x: columnAvailabeTableRef.keySet()){
			System.out.println(x);
			for(int i : columnAvailabeTableRef.get(x)){
				System.out.println("\t" + tableKeyRef.get(i));
			}
		}

		

	}
	
	private static boolean stackImp(String fullName, int i) {
		if(i > stack.size())
			return false;
		if(stack.contains(fullName) && i >= stack.size())
			return false;
		if(i == stack.size()){
			stack.push(fullName);
		}
		else if(i <= stack.size()){
			while(stack.size() > i)
				stack.pop();
			if(stack.contains(fullName))
					return false;
			else
				stack.push(fullName);					
		}
		if(map.containsKey(stack.get(0)+" ---> "+fullName)){
			map.put(stack.get(0)+" ---> "+fullName, getStack(map.get(stack.get(0)+" ---> "+fullName),stack));
		}
		else
			map.put(stack.get(0)+" ---> "+fullName,getStack(null,stack));
		return true;
	}
	
	private static void processLogic(String src, int i) {
		for (String srcpath : srcDestCol.get(src).keySet()) {
			if(stackImp(srcpath,i+1))
				processLogic(srcpath,i+1);
		}
	}
	
	static <T> boolean compareStacks(Stack<T> a, Stack<T> b) {
	    if (a.isEmpty() != b.isEmpty()) return false; 
	    if (a.isEmpty() && b.isEmpty()) return true; 
	    T element_a = a.pop(); 
	    T element_b = b.pop();
	    try {
	        if (((element_a==null) && (element_b!=null)) || (!element_a.equals(element_b)))
	            return false;
	        return compareStacks(a, b); 
	    } finally { // restore elements
	        a.push(element_a); 
	        b.push(element_b);
	    }
	}
	
	private static ArrayList<Stack<String>> getStack(ArrayList<Stack<String>> arrayList, Stack<String> stack2) {
		Stack<String> s = new Stack<String>();
		s.addAll(stack2);
		boolean flag = true;
		ArrayList<Stack<String>> arrayListNow = new ArrayList<Stack<String>>();
		if (arrayList != null)
			for (Stack<String> stack : arrayList) {
				if(compareStacks(s, stack))
					flag = false;
				arrayListNow.add(stack);
			}
		if(flag)
			arrayListNow.add(s);
		return arrayListNow;
	}
}
