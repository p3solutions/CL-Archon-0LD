package com.p3.archon.dboperations.dbmodel.enums;

public enum ParameterType {
	GROUP("Group"), KEYWORD("Keyword");

	private String value;

	ParameterType(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
