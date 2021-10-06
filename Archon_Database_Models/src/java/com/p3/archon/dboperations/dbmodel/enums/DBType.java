package com.p3.archon.dboperations.dbmodel.enums;

public enum DBType {
	SQL("Microsoft SQL Server"),
	ORACLE("Oracle"),
	MYSQL("MySQL"),
	DB2("DB2"),
	POSTGRESQL("PostgreSQL"),
	SYBASE("Sybase"),
	TERADATA("Teradata"),
	SHAREPOINT("Share Poinit"),
	DOCUMENTUM("Documentum"),
	LOTUSNOTES("Lotus Notes"),
	SALESFORCE("Sales Force"),
	AS400("AS400"), 
	MARIA("MariaDB"),
	;
	
	private String value;
	
	DBType(String value){
		this.value = value;
	}
	
	public String getValue(){
		return value;
	}
}
