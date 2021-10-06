package testing;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TestingMain3 {
	static Map<String, Integer> tableHirachy = new LinkedHashMap<String, Integer>();
	static Map<String, LinkedHashMap<String, Boolean>> tableExt = new LinkedHashMap<String, LinkedHashMap<String, Boolean>>();
	static int max = 0;

	static String DB_URL = "medtronic-mitg.my.salesforce.com";
	static String USER = "urjaswi.kumar@medtronic.com";
	static String PASS = "Oct@11kvrRalnVNtSuRzpm3fHEEuH5tLG";

	public static String getURL() {
		return "jdbc:sforce://" + DB_URL;
	}

	public static void main(final String[] args) throws Exception {
		
		Properties props = new Properties();
		props.put("connectTimeout", "2000");
		props.put("user", USER);
		props.put("password", PASS);

		final Connection connection = DriverManager.getConnection(getURL(),props);
		DatabaseMetaData x = connection.getMetaData();

		List<String> allTables = new ArrayList<String>();
		ResultSet rs = x.getTables(null, null, "%", null);
		while (rs.next()) {
			allTables.add(rs.getString(3));
		}
		rs.close();
		Collections.sort(allTables);
		System.out.println(allTables);
		System.out.println(allTables.size());
		for (String table : allTables) {
			StringBuffer sb = new StringBuffer();
			System.out.println("Columns Info for \"" + table + "\"");
			boolean sx = false;
			int count = 0;
			
			// Column
			ResultSet rs1 = x.getColumns(null, null, table, null);
			while (rs1.next()) {
				if(!sx){
					count = rs1.getMetaData().getColumnCount();
					System.out.print(" | ");
					for(int i = 1; i <= count ; i++){
						System.out.print("-------------------------------------------------------------------------------------".substring(0,50));
						System.out.print(" | ");
					}
					System.out.println();
					System.out.print(" | ");
					for(int i = 1; i <= count; i++){
						System.out.print(rightPadding(rs1.getMetaData().getColumnLabel(i),50));
						System.out.print(" | ");
					}
					System.out.println();
					System.out.print(" | ");
					for(int i = 1; i <= count; i++){
						System.out.print("-------------------------------------------------------------------------------------".substring(0,50));
						System.out.print(" | ");
					}
					System.out.println();
					sx = true;
				}
				System.out.print(" | ");
				for(int i =1; i <= count; i++){
					if(i == 4)
						sb.append(",").append(rs1.getString(i));
					System.out.print(rightPadding(rs1.getString(i),50));
					System.out.print(" | ");
				}
				System.out.println();
				// rs1.getString(4);
			}
			System.out.print(" | ");
			for(int i = 1; i <= count; i++){
				System.out.print("-------------------------------------------------------------------------------------".substring(0,50));
				System.out.print(" | ");
			}
			System.out.println();
			rs1.close();
			
			// Query
			String query = "select "+ sb.substring(1) + " from " + table + " LIMIT 10";
			System.out.println(query);
			Statement stmt = connection.createStatement();
	        ResultSet rs2 = stmt.executeQuery(query);
	        sx = false;
			count = 0;
	        while (rs2.next()) {
	        	if(!sx){
					count = rs2.getMetaData().getColumnCount();
					System.out.print(" | ");
					for(int i = 1; i <= count ; i++){
						System.out.print("-------------------------------------------------------------------------------------".substring(0,50));
						System.out.print(" | ");
					}
					System.out.println();
					System.out.print(" | ");
					for(int i = 1; i <= count; i++){
						System.out.print(rightPadding(rs2.getMetaData().getColumnLabel(i),50));
						System.out.print(" | ");
					}
					System.out.println();
					System.out.print(" | ");
					for(int i = 1; i <= count; i++){
						System.out.print("-------------------------------------------------------------------------------------".substring(0,50));
						System.out.print(" | ");
					}
					System.out.println();
					sx = true;
				}
				System.out.print(" | ");
				for(int i =1; i <= count; i++){
					System.out.print(rightPadding(rs2.getString(i),50));
					System.out.print(" | ");
				}
				System.out.println();
			}
			System.out.print(" | ");
			for(int i = 1; i <= count; i++){
				System.out.print("-------------------------------------------------------------------------------------".substring(0,50));
				System.out.print(" | ");
			}
			System.out.println();
			rs2.close();
		}
		
		connection.close();
	}
	
	public static String rightPadding(String str, int num) {
		return String.format("%1$-" + num + "s", str);
	}
}
