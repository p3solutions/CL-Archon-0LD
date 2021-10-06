package malik.sample;

import static us.fatehi.commandlineparser.CommandLineUtility.applyApplicationLogLevel;

import java.sql.Connection;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.logging.Level;

import javax.sql.DataSource;

import schemacrawler.schema.Catalog;
import schemacrawler.schema.ForeignKey;
import schemacrawler.schema.ForeignKeyColumnReference;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;
import schemacrawler.schema.View;
import schemacrawler.schemacrawler.DatabaseConnectionOptions;
import schemacrawler.schemacrawler.ExcludeAll;
import schemacrawler.schemacrawler.RegularExpressionInclusionRule;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.utility.SchemaCrawlerUtility;

public class LiveArchivalStart {
static Map<String, Integer> tableHirachy = new LinkedHashMap<String, Integer>();
static Map<String, LinkedHashMap<String,Boolean>> tableExt = new LinkedHashMap<String, LinkedHashMap<String,Boolean>>();
static LinkedHashMap<String, Boolean> thisTable;
static LinkedHashMap<String, LinkedHashMap<String,LinkedHashMap<String,String>>> joins = new LinkedHashMap<String, LinkedHashMap<String,LinkedHashMap<String,String>>>();;

	public static void main(final String[] args) throws Exception {

		applyApplicationLogLevel(Level.OFF);
		final DataSource dataSource = new DatabaseConnectionOptions(
				"jdbc:sqlserver://localhost:1433;database=CLAIMS_SYS");
		final Connection connection = dataSource.getConnection("admin", "secret");

		final SchemaCrawlerOptions options = new SchemaCrawlerOptions();
		options.setRoutineInclusionRule(new ExcludeAll());
		options.setSchemaInclusionRule(new RegularExpressionInclusionRule("CLAIMS_SYS.dbo.*"));

		System.out.println("Live Archival");
		
		final Catalog catalog = SchemaCrawlerUtility.getCatalog(connection, options);
		String tableName = "CLAIMS_SYS.dbo.CLAIM";
		
		processLogic(catalog,tableName);
		int min = 0;
		int max = 0;
		for (String tableName1 : tableHirachy.keySet()) {
			if(tableHirachy.get(tableName1) > max)
				max = tableHirachy.get(tableName1);
			if(tableHirachy.get(tableName1) < min)
				min = tableHirachy.get(tableName1);
		}
		
		for (int i = min; i <= max; i++) {
			System.out.println("Level " + i);
			System.out.println("-----------");
			for (String tableName1 : tableHirachy.keySet()) {
				if(tableHirachy.get(tableName1) == i)
					System.out.print(tableName1 + "\t");
			}
			System.out.println("\n");
		}
		
		System.out.println(tableName);
		System.out.println("---------------------");
		for (String tableJoins : tableExt.get(tableName).keySet()) {
			System.out.println(tableJoins);
		}
		System.out.println();
		System.out.println();
		
		for(String x:joins.keySet()){
			System.out.println(x);
			System.out.println("------------------------------------------");
			StringBuffer sb = new StringBuffer();
			for (String y : joins.get(x).keySet()) {
				System.out.println("\t" + y);
				if(!sb.toString().equals("")){
					sb.append(" or ");
				}
				sb.append("(");
				boolean flag = false;
				for (String z : joins.get(x).get(y).keySet()) {
					if(flag)
						sb.append(" and ");
					sb.append(joins.get(x).get(y).get(z));
					flag = true;
				}
				sb.append(")");
			}
			System.out.println(sb.toString());
			System.out.println();
		}
	}

	private static Stack<String> stack = new Stack<String>();
	
	private static void stackImp(String fullName, int j ){
		System.out.println(" ----- >" + j + " ---- " + fullName);
		if(j==0)
			stack.push(fullName);
		if(stack.contains(fullName))
			return;
		else
			stack.push(fullName);
		while(j < stack.size())
			stack.pop();
		System.out.println("-------------------");
		System.out.println(stack);
		System.out.println("-------------------");
	}
	private static void putTable(String fullName, int i, boolean force) {
		//System.out.println(fullName + "   " + i);
			if(tableHirachy.get(fullName) == null || force)
				tableHirachy.put(fullName, i);
			else
				if(tableHirachy.get(fullName) > 0 && tableHirachy.get(fullName) < i)
					tableHirachy.put(fullName,i);
				else if(tableHirachy.get(fullName) < 0 && tableHirachy.get(fullName) > i)
					tableHirachy.put(fullName,i);
			//System.out.println(tableHirachy);
	}

	private static int tableHirachysearch(String fullName) {
		if(tableHirachy.get(fullName) != null)
			return tableHirachy.get(fullName);
		else return 0;
	}
	
	
	private static Map<String, Integer> processLogic(Catalog catalog, String tableNeed) {
		for (final Schema schema : catalog.getSchemas()) {
			for (final Table table : catalog.getTables(schema)) {
				if(table instanceof View || !table.getFullName().equals(tableNeed))
					continue;					
				thisTable = new LinkedHashMap<String,Boolean>();
				stackImp(table.getFullName(), 0);
				int j = 1;
				putTable(table.getFullName(),0,true);
				Collection<ForeignKey> x = table.getForeignKeys();
					for (Iterator<ForeignKey> iterator = x.iterator(); iterator.hasNext();) {
						ForeignKey foreignKey = (ForeignKey) iterator.next();
						List<ForeignKeyColumnReference> y = (foreignKey.getColumnReferences());
						for (ForeignKeyColumnReference foreignKeyColumnReference : y) {
							LinkedHashMap<String,LinkedHashMap<String,String>> list = new LinkedHashMap<String,LinkedHashMap<String,String>>();
							if(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().equals(table))
								stackImp(foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),j);
							else
								stackImp(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName(),j);
							if(joins.containsKey(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName() + "_" + foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName())){
								list = joins.get(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName() + "_" + foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName());
								LinkedHashMap<String,String> keylist = list.get(foreignKey.getName());
								if(keylist == null)
									keylist = new LinkedHashMap<String,String>();
								keylist.put(foreignKeyColumnReference.getPrimaryKeyColumn().getFullName()+"_"+foreignKeyColumnReference.getForeignKeyColumn().getFullName(),foreignKeyColumnReference.getPrimaryKeyColumn().getFullName() + "=" + foreignKeyColumnReference.getForeignKeyColumn().getFullName());
								list.put(foreignKey.getName(), keylist);
								joins.put(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName() + "_" + foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),list);
								/*System.out.println(table);*/
								continue;
							}
							if(foreignKeyColumnReference.getForeignKeyColumn().getParent().equals(foreignKeyColumnReference.getPrimaryKeyColumn().getParent()));
							else if(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().equals(table)){
								LinkedHashMap<String,String> keylist = list.get(foreignKey.getName());
								if(keylist == null)
									keylist = new LinkedHashMap<String,String>();
								keylist.put(foreignKeyColumnReference.getPrimaryKeyColumn().getFullName()+"_"+foreignKeyColumnReference.getForeignKeyColumn().getFullName(),foreignKeyColumnReference.getPrimaryKeyColumn().getFullName() + "=" + foreignKeyColumnReference.getForeignKeyColumn().getFullName());
								list.put(foreignKey.getName(), keylist);
								joins.put(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName() + "_" + foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),list);
								putTable(foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),tableHirachysearch(table.getFullName())+1,true);
								/*System.out.println(table);*/
								getChild(catalog,foreignKeyColumnReference.getForeignKeyColumn().getParent(),tableHirachysearch(table.getFullName())+1,j);
								thisTable.put(foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),false);
								}
							else{
								LinkedHashMap<String,String> keylist = list.get(foreignKey.getName());
								if(keylist == null)
									keylist = new LinkedHashMap<String,String>();
								keylist.put(foreignKeyColumnReference.getPrimaryKeyColumn().getFullName()+"_"+foreignKeyColumnReference.getForeignKeyColumn().getFullName(),foreignKeyColumnReference.getPrimaryKeyColumn().getFullName() + "=" + foreignKeyColumnReference.getForeignKeyColumn().getFullName());
								list.put(foreignKey.getName(), keylist);
								joins.put(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName() + "_" + foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),list);
								putTable(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName(),tableHirachysearch(table.getFullName())-1,true);
								/*System.out.println(table);*/
								getParent(catalog,foreignKeyColumnReference.getPrimaryKeyColumn().getParent(),tableHirachysearch(table.getFullName())-1,j);
								thisTable.put(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName(),false);
							}
							
						}
				}
				tableExt.put(table.getFullName(), thisTable);
			}
		}
		return tableHirachy;
	}

	private static void getParent(Catalog catalog, Table parent, int i, int j) {
		for (final Schema schema : catalog.getSchemas()) {
			for (final Table table : catalog.getTables(schema)) {
				if(table instanceof View)
					continue;
				if(table.getName().equals(parent.getName())){
				Collection<ForeignKey> x = table.getForeignKeys();
					for (Iterator<ForeignKey> iterator = x.iterator(); iterator.hasNext();) {
						ForeignKey foreignKey = (ForeignKey) iterator.next();
						List<ForeignKeyColumnReference> y = (foreignKey.getColumnReferences());
						for (ForeignKeyColumnReference foreignKeyColumnReference : y) {
							LinkedHashMap<String,LinkedHashMap<String,String>> list = new LinkedHashMap<String,LinkedHashMap<String,String>>();
							if(table.getName().equals("PROVIDER")){
								System.out.println("Provider");
							}
							if(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().equals(table))
								stackImp(foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),j+1);
							else
								stackImp(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName(),j+1);
							if(joins.containsKey(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName() + "_" + foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName())){
								list = joins.get(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName() + "_" + foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName());
								LinkedHashMap<String,String> keylist = list.get(foreignKey.getName());
								if(keylist == null)
									keylist = new LinkedHashMap<String,String>();
								keylist.put(foreignKeyColumnReference.getPrimaryKeyColumn().getFullName()+"_"+foreignKeyColumnReference.getForeignKeyColumn().getFullName(),foreignKeyColumnReference.getPrimaryKeyColumn().getFullName() + "=" + foreignKeyColumnReference.getForeignKeyColumn().getFullName());
								list.put(foreignKey.getName(), keylist);
								joins.put(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName() + "_" + foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),list);
								/*for(int c = 0; c <j;c++)
									System.out.print("\t");
								System.out.println(table);*/
								continue;
							}
							if(foreignKeyColumnReference.getForeignKeyColumn().getParent().equals(foreignKeyColumnReference.getPrimaryKeyColumn().getParent()));
							else if(foreignKeyColumnReference.getForeignKeyColumn().getParent().equals(table)){
								LinkedHashMap<String,String> keylist = list.get(foreignKey.getName());
								if(keylist == null)
									keylist = new LinkedHashMap<String,String>();
								keylist.put(foreignKeyColumnReference.getPrimaryKeyColumn().getFullName()+"_"+foreignKeyColumnReference.getForeignKeyColumn().getFullName(),foreignKeyColumnReference.getPrimaryKeyColumn().getFullName() + "=" + foreignKeyColumnReference.getForeignKeyColumn().getFullName());
								list.put(foreignKey.getName(), keylist);
								joins.put(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName() + "_" + foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),list);
								putTable(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName(),tableHirachysearch(table.getFullName())-1, false);
								/*for(int c = 0; c <j;c++)
									System.out.print("\t");
								System.out.println(table);*/
								getParent(catalog,foreignKeyColumnReference.getPrimaryKeyColumn().getParent(),tableHirachysearch(table.getFullName())-1,j+1);
								thisTable.put(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName(),false);
							}
							else if(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().equals(table)){
								LinkedHashMap<String,String> keylist = list.get(foreignKey.getName());
								if(keylist == null)
									keylist = new LinkedHashMap<String,String>();
								keylist.put(foreignKeyColumnReference.getPrimaryKeyColumn().getFullName()+"_"+foreignKeyColumnReference.getForeignKeyColumn().getFullName(),foreignKeyColumnReference.getPrimaryKeyColumn().getFullName() + "=" + foreignKeyColumnReference.getForeignKeyColumn().getFullName());
								list.put(foreignKey.getName(), keylist);
								joins.put(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName() + "_" + foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),list);
								putTable(foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),tableHirachysearch(table.getFullName())+1, false);
								/*for(int c = 0; c <j;c++)
									System.out.print("\t");
								System.out.println(table);*/
								getChild(catalog,foreignKeyColumnReference.getForeignKeyColumn().getParent(),tableHirachysearch(table.getFullName())+1,j+1);
								thisTable.put(foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),false);
							}
							
						}
					}
				}
			}
		}
	}
	
	private static void getChild(Catalog catalog, Table parent, int i, int j) {
		for (final Schema schema : catalog.getSchemas()) {
			for (final Table table : catalog.getTables(schema)) {
				if(table instanceof View)
					continue;
				if(table.getName().equals(parent.getName())){
				Collection<ForeignKey> x = table.getForeignKeys();
					for (Iterator<ForeignKey> iterator = x.iterator(); iterator.hasNext();) {
						ForeignKey foreignKey = (ForeignKey) iterator.next();
						List<ForeignKeyColumnReference> y = (foreignKey.getColumnReferences());
						for (ForeignKeyColumnReference foreignKeyColumnReference : y) {
							LinkedHashMap<String,LinkedHashMap<String,String>> list = new LinkedHashMap<String,LinkedHashMap<String,String>>();
							if(table.getName().equals("PROVIDER")){
								System.out.println("Provider");
							}if(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().equals(table))
								stackImp(foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),j+1);
							else
								stackImp(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName(),j+1);
							if(joins.containsKey(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName() + "_" + foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName())){
								list = joins.get(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName() + "_" + foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName());
								LinkedHashMap<String,String> keylist = list.get(foreignKey.getName());
								if(keylist == null)
									keylist = new LinkedHashMap<String,String>();
								keylist.put(foreignKeyColumnReference.getPrimaryKeyColumn().getFullName()+"_"+foreignKeyColumnReference.getForeignKeyColumn().getFullName(),foreignKeyColumnReference.getPrimaryKeyColumn().getFullName() + "=" + foreignKeyColumnReference.getForeignKeyColumn().getFullName());
								list.put(foreignKey.getName(), keylist);
								joins.put(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName() + "_" + foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),list);
								/*for(int c = 0; c <j;c++)
									System.out.print("\t");
								System.out.println(table);*/
								continue;
							}
							if(foreignKeyColumnReference.getForeignKeyColumn().getParent().equals(foreignKeyColumnReference.getPrimaryKeyColumn().getParent()));
							else if(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().equals(table)){
								LinkedHashMap<String,String> keylist = list.get(foreignKey.getName());
								if(keylist == null)
									keylist = new LinkedHashMap<String,String>();
								keylist.put(foreignKeyColumnReference.getPrimaryKeyColumn().getFullName()+"_"+foreignKeyColumnReference.getForeignKeyColumn().getFullName(),foreignKeyColumnReference.getPrimaryKeyColumn().getFullName() + "=" + foreignKeyColumnReference.getForeignKeyColumn().getFullName());
								list.put(foreignKey.getName(), keylist);
								joins.put(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName() + "_" + foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),list);
								putTable(foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),tableHirachysearch(table.getFullName())+1, false);
								/*for(int c = 0; c <j;c++)
									System.out.print("\t");
								System.out.println(table);*/
								getChild(catalog,foreignKeyColumnReference.getForeignKeyColumn().getParent(),tableHirachysearch(table.getFullName())+1, j + 1);
								thisTable.put(foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),false);
							}
							else if(foreignKeyColumnReference.getForeignKeyColumn().getParent().equals(table)){
								LinkedHashMap<String,String> keylist = list.get(foreignKey.getName());
								if(keylist == null)
									keylist = new LinkedHashMap<String,String>();
								keylist.put(foreignKeyColumnReference.getPrimaryKeyColumn().getFullName()+"_"+foreignKeyColumnReference.getForeignKeyColumn().getFullName(),foreignKeyColumnReference.getPrimaryKeyColumn().getFullName() + "=" + foreignKeyColumnReference.getForeignKeyColumn().getFullName());
								list.put(foreignKey.getName(), keylist);
								joins.put(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName() + "_" + foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName(),list);
								putTable(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName(),tableHirachysearch(table.getFullName())-1, false);
								/*for(int c = 0; c <j;c++)
									System.out.print("\t");
								System.out.println(table);*/
								getParent(catalog,foreignKeyColumnReference.getPrimaryKeyColumn().getParent(),tableHirachysearch(table.getFullName())-1, j+ 1);
								thisTable.put(foreignKeyColumnReference.getPrimaryKeyColumn().getParent().getFullName(),false);
							}
						}
					}
				}
			}
		}
	}
}
