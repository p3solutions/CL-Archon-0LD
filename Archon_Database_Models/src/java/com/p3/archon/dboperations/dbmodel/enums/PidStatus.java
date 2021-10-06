package com.p3.archon.dboperations.dbmodel.enums;

public enum PidStatus {
	BEGIN("Record added in db"), SCHEDULED("Scheduled"), CANCELLED("killed the process"), FAILED(
			"Failed to kill the process"), DONE("Job completed");

	private String value;

	PidStatus(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

}
