package com.p3.archon.dboperations.dbmodel.enums;

public enum FlatFileJobType {
	TABLE("Table Extraction"), SIP("SIP Extraction");
	private String value;

	FlatFileJobType(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
