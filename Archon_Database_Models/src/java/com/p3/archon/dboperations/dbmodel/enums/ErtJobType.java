package com.p3.archon.dboperations.dbmodel.enums;

public enum ErtJobType {
	TABLE("Table Extraction"), 
	RECORD("Data Record Extraction"), 
	SIP("SIP Extraction"), 
	QUERY("QUERY Extraction");

	private String value;
	
	ErtJobType(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
