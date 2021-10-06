package com.p3.archon.dboperations.dbmodel.enums;

public enum ToolCategory {

	AUTHENTIATION_TOOL("Authentication"),
	ADMIN_TOOLS("Admin Tools"),
	SHARED_TOOLS("Management Tools"),
	EXTRACTION_TOOLS("Extraction Tools"),
	IA_TOOLS("IA Specific Tools"),
	ANALYSIS_TOOLS("Analysis Tools"),
	WORKFLOW_TOOLS("Workflow Tools");

	private String value;

	ToolCategory(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

}
