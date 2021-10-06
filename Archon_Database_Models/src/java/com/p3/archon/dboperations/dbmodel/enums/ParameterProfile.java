package com.p3.archon.dboperations.dbmodel.enums;

public enum ParameterProfile {
	PREDEFINED("Predefined"), USERDEFINED("UserDefined");

	private String value;

	ParameterProfile(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
