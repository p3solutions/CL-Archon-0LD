package malik.trials.backup;

import static us.fatehi.commandlineparser.CommandLineUtility.applyApplicationLogLevel;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;

import javax.sql.DataSource;

import schemacrawler.schema.Catalog;
import schemacrawler.schema.Column;
import schemacrawler.schema.ForeignKey;
import schemacrawler.schema.ForeignKeyColumnReference;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;
import schemacrawler.schema.View;
import schemacrawler.schemacrawler.DatabaseConnectionOptions;
import schemacrawler.schemacrawler.ExcludeAll;
import schemacrawler.schemacrawler.RegularExpressionInclusionRule;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.utility.MetaDataUtility;
import schemacrawler.utility.SchemaCrawlerUtility;

public class TryLiveGraph{

	private static Stack<String> stack = new Stack<String>();
	private static Map<String,TreeSet<String>> keymap = new TreeMap<String,TreeSet<String>>();
	private static Map<String,TreeMap<Integer,Column>> colmap = new TreeMap<String,TreeMap<Integer,Column>>();
	private static Map<String,TreeMap<String,TreeSet<String>>> relationshipmap = new TreeMap<String,TreeMap<String,TreeSet<String>>>();
	private static Map<String,TreeMap<String,String>> primary2Foreignmap = new TreeMap<String,TreeMap<String,String>>();
	
	private static String TABLENAME = "CLAIMS_SYS.dbo.CLAIM";
	private static String CONDITION = "1=1";
	private static String EXCLUSION = "";
	private static TreeSet<String> exclusionList = new TreeSet<String>();
	
	public static void main(String[] args) throws Exception {
		applyApplicationLogLevel(Level.OFF);
		final DataSource dataSource = new DatabaseConnectionOptions(
				"jdbc:sqlserver://localhost:1433;database=CLAIMS_SYS");
		final Connection connection = dataSource.getConnection("admin", "secret");

		final SchemaCrawlerOptions options = new SchemaCrawlerOptions();
		options.setRoutineInclusionRule(new ExcludeAll());
		options.setSchemaInclusionRule(new RegularExpressionInclusionRule("CLAIMS_SYS.dbo.*"));

		System.out.println("Live Archival");
		
		final Catalog catalog = SchemaCrawlerUtility.getCatalog(connection, options);
		
		String[] exclusionListArray = EXCLUSION.split(";");
		for (int i = 0; i < exclusionListArray.length; i++) {
			exclusionList.add(exclusionListArray[i]);
		}
		stackImp(TABLENAME, 0);
		processLogic(catalog,TABLENAME,0);
		System.out.println();
		System.out.println();
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
		
		
		System.out.println("---------------KEYS-----------------");
		for (String fkname : keymap.keySet()) {
			System.out.println(fkname);
			System.out.println("--------------");
			for (String string : keymap.get(fkname)) {
				System.out.println(string);
			}
			System.out.println();
		}
		
		
		System.out.println("------------REL------------------");
		for (String src : relationshipmap.keySet()) {
			System.out.println(src);
			System.out.println("------------------");
			for (String dest : relationshipmap.get(src).keySet()) {
				System.out.println("\t"+dest);
				for (String string : relationshipmap.get(src).get(dest)) {
					System.out.println("\t\t-->" + string);
				}
			}
			System.out.println();
		}
		
		System.out.println("----------------SQL---------------------");
		for (String string : map.keySet()) {
			System.out.println(string);
			System.out.println("------------------");
			StringBuffer main = new StringBuffer("");
			for (Stack<String> stack : map.get(string)) {
				StringBuffer sb = new StringBuffer();
				int size = stack.size();
				for (int i = size-1; i >= 0; i--) {
					if(i==size-1)
						sb.append(" SELECT DISTINCT ").append(getColumns(stack.get(i))).append(" \n\tFROM ").append(stack.get(i));
					else {
						sb.append(" \n\t\tINNER JOIN ").append(stack.get(i)).append(" ON\n\t\t\t ").append(getJoin(stack.get(i),stack.get(i+1)));
					}
				}
				sb.append(" \n\twhere ").append(CONDITION);
				if(main.toString().equals(""))
					main.append("SELECT DISTINCT * FROM ( ").append(sb.toString());
				else
					main.append(" \nUNION ").append(sb.toString());
			}
			if(!main.toString().equals(""))
				main.append(") thistable ");
			System.out.println(main.toString());
			System.out.println();
			System.out.println();
		}
		
		
		graphGenerator();
		
	}
	
	
	private static String getArrowHeads(String card) {
		switch(card){
		case "(0..1)":
			return "arrowhead=\"teeodot\" arrowtail=\"teetee\"";
		case "(0..many)":
			return "arrowhead=\"crowodot\" arrowtail=\"teetee\"";
		case "(1..1)":
			return "arrowhead=\"teetee\" arrowtail=\"teetee\"";
		default:
			return "arrowhead=\"box\" arrowtail=\"box\"";
		}				
	}


	private static void graphGenerator() {
		System.out.println("------------SELECTED REL GRAPH------------------");
		StringBuffer graph = new StringBuffer();
		graph.append("digraph G{node [shape=box]; ");
		for (String src : primary2Foreignmap.keySet()) {
			for (String dest : primary2Foreignmap.get(src).keySet())
				graph.append(" ").append(src).append(" -> ").append(dest).append(" [style=\"solid\" dir=\"both\" ").append(getArrowHeads(primary2Foreignmap.get(src).get(dest))).append("]; ");
		}
		if(primary2Foreignmap.isEmpty()){
			graph.append(" TABLENAME ");
		}
		graph.append("}");
		System.out.println(graph.toString());
		
	}

	private static String getColumns(String table) {
		StringBuffer sb = new StringBuffer("");
		for (int pos : colmap.get(table).keySet()) {
			if(sb.toString().equals(""))
				sb.append(colmap.get(table).get(pos));
			else
				sb.append(",").append(colmap.get(table).get(pos).getFullName());
		}
		return sb.toString();
	}

	private static String getJoin(String src, String dest) {
		StringBuffer sb = new StringBuffer("");
		for(String joinkey :relationshipmap.get(src).get(dest)){
			if(sb.toString().equals(""))
				sb.append("(");
			else
				sb.append(" or\n\t\t\t (");
			StringBuffer sb1 = new StringBuffer("");
			for(String join : keymap.get(joinkey)){
				if(sb1.toString().equals(""))
					sb1.append(" ").append(join).append(" ");
				else
					sb1.append(" and ").append(join).append(" ");
			}
			sb.append(sb1.toString()).append(")");
		}
		return sb.toString();
	}

	private static void processLogic(Catalog catalog, String tableNeed, int j) {
		for (final Schema schema : catalog.getSchemas()) {
			for (final Table table : catalog.getTables(schema)) {
				if(table instanceof View || !table.getFullName().equals(tableNeed) || isInExclusionList(table.getFullName()))
					continue;
				getColumnsInfo(table);
				Collection<ForeignKey> x = table.getForeignKeys();
				boolean flag = true;
				do {for (Iterator<ForeignKey> iterator = x.iterator(); iterator.hasNext();) {
					ForeignKey foreignKey = (ForeignKey) iterator.next();
					List<ForeignKeyColumnReference> y = (foreignKey.getColumnReferences());
						for (ForeignKeyColumnReference foreignKeyColumnReference : y) {
							if(isInExclusionList(foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName()) || isInExclusionList(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName()))
								continue;							
							addJoin(foreignKeyColumnReference, foreignKey.getName(),table.getFullName());
							addRelationShips(foreignKeyColumnReference, foreignKey.getName(),table.getFullName(),MetaDataUtility.findForeignKeyCardinality(foreignKey).toString());
							if(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().equals(table)){
								if(stackImp(foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),j+1))
									processLogic(catalog,foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),j+1);
							}
							else{
								if(stackImp(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName(),j+1))
									processLogic(catalog,foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName(),j+1);
							}
						}
					}
				flag= false;
				}while(flag);
			}
		}
	}

	private static boolean isInExclusionList(String fullName) {
		if(exclusionList.contains(fullName))
			return true;
		return false;
	}


	private static void getColumnsInfo(Table table) {
		if(colmap.containsKey(table.getFullName()))
			return;
		else {
			TreeMap<Integer, Column> cols = new TreeMap<Integer,Column>();
			for (Column col : table.getColumns()) 
				cols.put(col.getOrdinalPosition(), col);
			colmap.put(table.getFullName(),cols );			
		}
	}

	private static void addRelationShips(ForeignKeyColumnReference join, String keyName, String srcTable, String card) {
		Table fk = join.getForeignKeyColumn().getParent();
		Table pk = join.getPrimaryKeyColumn().getParent();
		String destTable = fk.getFullName().equals(srcTable)?pk.getFullName():fk.getFullName();
		
		TreeMap<String,TreeSet<String>> rel = new TreeMap<String,TreeSet<String>>();
		if(relationshipmap.containsKey(srcTable)){
			rel = relationshipmap.get(srcTable);
		}
		
		TreeSet<String> keyList = new TreeSet<String>();
		if(rel.containsKey(destTable)){
			keyList = rel.get(destTable);
		}
		
		keyList.add(keyName);
		
		rel.put(destTable, keyList);
		relationshipmap.put(srcTable, rel);
		
		if(pk.getFullName().equals(srcTable)){
			TreeMap<String,String> fktables = new TreeMap<String,String>();
			if(primary2Foreignmap.containsKey(pk.getName())){
				fktables = primary2Foreignmap.get(pk.getName());
			}
			fktables.put(fk.getName(),card);
			primary2Foreignmap.put(pk.getName(), fktables);
		}
	}

	private static void addJoin(ForeignKeyColumnReference join, String keyName, String table) {
		TreeSet<String> joinKeys = new TreeSet<String>();
		Column fk = join.getForeignKeyColumn();
		Column pk = join.getPrimaryKeyColumn();
		
		if(keymap.containsKey(keyName))
			joinKeys = keymap.get(keyName);

		StringBuffer sb = new StringBuffer();
		sb.append(pk.getFullName()).append("=").append(fk.getFullName());
		joinKeys.add(sb.toString());
		keymap.put(keyName, joinKeys);
	}
	
	private static Map<String, ArrayList<Stack<String>>> map = new TreeMap<String,ArrayList<Stack<String>>>();
	
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
