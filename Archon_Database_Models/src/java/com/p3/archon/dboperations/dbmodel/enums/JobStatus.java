package com.p3.archon.dboperations.dbmodel.enums;

public enum JobStatus {
	SCHEDULED("Scheduled"), INPROGRESS("In Progress"), COMPLETED("Completed"), FAILED("Failed"), CANCELLED(
			"User/Admin Cancelled"),SCHEDULED_TO_STOP("Scheduled to stop");

	private String value;

	JobStatus(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
