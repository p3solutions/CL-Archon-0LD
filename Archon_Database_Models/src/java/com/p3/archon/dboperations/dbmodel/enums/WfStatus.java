package com.p3.archon.dboperations.dbmodel.enums;

public enum WfStatus {
	ANALYZING("Analyzing"), COMPLETED("Analysis Completed"), FAILED("Analysis Failed"), CANCELLED(
			"Analysis Cancelled");

	private String value;

	WfStatus(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
