package com.p3.archon.dboperations.dbmodel.enums;

public enum JobInterval {
	IMMEDIATE("Immediate", "Immediate", "Immediate"), ONCE("Scheduled", "Once", "Schedule Once"), DAYS("Scheduled",
			"Days", "Daily job"), WEEKS("Scheduled", "Weeks", "Weekly job"), MONTHS("Scheduled", "Months",
					"Monthly job"), HOURS("Scheduled", "Hours", "Hourly job"), MINUTES("Scheduled", "Minutes", "Minutes job");
	private String value;
	private String key;
	private String scheule;

	JobInterval(String value, String key, String scheule) {
		this.value = value;
		this.key = key;
		this.scheule = scheule;
	}

	public String getValue() {
		return value;
	}

	public String getKey() {
		return key;
	}

	public String getSchedule() {
		return scheule;
	}

	public static JobInterval getInterval(String intervalType) {
		if (intervalType.equals(JobInterval.IMMEDIATE.getKey()))
			return JobInterval.IMMEDIATE;
		else if (intervalType.equals(JobInterval.ONCE.getKey()))
			return JobInterval.ONCE;
		else if (intervalType.equals(JobInterval.MINUTES.getKey()))
			return JobInterval.MINUTES;
		else if (intervalType.equals(JobInterval.HOURS.getKey()))
			return JobInterval.HOURS;
		else if (intervalType.equals(JobInterval.DAYS.getKey()))
			return JobInterval.DAYS;
		else if (intervalType.equals(JobInterval.WEEKS.getKey()))
			return JobInterval.WEEKS;
		else if (intervalType.equals(JobInterval.MONTHS.getKey()))
			return JobInterval.MONTHS;
		return null;
	}
}
