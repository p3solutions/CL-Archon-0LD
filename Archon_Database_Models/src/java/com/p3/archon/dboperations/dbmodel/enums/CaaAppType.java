package com.p3.archon.dboperations.dbmodel.enums;

public enum CaaAppType {
	TEST("TEST"), SAP("SAP"), PS("PS"), JDE("JDE"), INVALID("NA"), OEBS("OEBS"),;

	private String value;

	CaaAppType(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	public static CaaAppType getAppType(String apptype) {
		switch (apptype.toLowerCase()) {
		case "sap":
			return SAP;
		case "ps":
			return PS;
		case "jde":
			return JDE;
		case "oebs":
			return OEBS;
		case "test":
			return TEST;
		default:
			return INVALID;
		}
	}
}
