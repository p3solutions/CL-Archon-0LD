package com.p3.archon.dboperations.dbmodel.enums;

public enum JobRunType {
	LOCAL("Local"),SERVER("Server");

	private String value;

	JobRunType(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
