package com.p3.archon.dboperations.dbmodel.enums;

public enum ScheduleStatus {
	
	SCHEDULED("Scheduled"), INPROGRESS("In Progress"), COMPLETED("Completed"), FAILED("Failed"), CANCELLED("User/Admin Stopped");

	private String value;

	ScheduleStatus(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

}
