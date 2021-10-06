package com.p3.archon.dboperations.dbmodel.enums;

public enum CAAJobType {
	EXTRACT_DATA("quickdump"), GENERATE_METADATA("metadata"), START_ANALYSIS("ANALYSIS OPERATION"), INVALID("NA"),;

	private String value;

	CAAJobType(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	public static CAAJobType getAppType(String apptype) {
		switch (apptype.toLowerCase()) {
		case "metadata":
			return GENERATE_METADATA;
		case "analysis operation":
			return START_ANALYSIS;
		case "quickdump":
			return EXTRACT_DATA;
		default:
			return INVALID;
		}
	}
}
