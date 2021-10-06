package com.p3.archon.dboperations.dbmodel.enums;

public enum ToolName {
	TOOL_AUDIT_NAME("Audit"), 
	
	// ANALYSIS
	TOOL_METALYZER_NAME("Metalyzer"), 
	TOOL_AS400_METALYZER_NAME("AS400 Metalyzer"),
	TOOL_META_METALYZER_NAME("Metadata Metalyzer"),
	
	// EXTRACTION
	TOOL_RDBMS_EXTRACTOR_NAME("RDBMS Extractor"),
	TOOL_ERT_EXTRACTOR_NAME("ERT"), 
	TOOL_BLOB_EXTRACTOR_NAME("Blob Extractor"),	
	TOOL_AS400_EXTRACTOR_NAME("AS400 Extractor"),
	
	TOOL_SHARE_POINT_EXTRACTOR_NAME("Share Point Extractor"),
	TOOL_FILE_SHARE_EXTRACTOR_NAME("File Share Extractor"),
	TOOL_DOCUMENTUM_EXTRACTOR_NAME("Documentum Extractor"),
	
	TOOL_LOTUS_NOTES_EXTRACTOR_NAME("Lotus Notes Extractor"),
	TOOL_FLAT_FILE_GENERATOR_NAME("Flat File Generator"), 
	
	TOOL_SALES_FORCE_EXTRACTOR_NAME("Sales Force Extractor"), //not in use
	TOOL_XML_FILE_EXTRACTOR_NAME("XML File Extractor"),
	
	// IA
	TOOL_AA_IA_NAME("IA App Automation"),
	TOOL_ING_IA_NAME("IA Data Ingester"),
	TOOL_COC_IA_NAME("Chain of Custody Validator"),
	TOOL_CSB_IA_NAME("IA Ad-Hoc Query Builder"),
	TOOL_QE_IA_NAME("IA Query Screen Builder"), //not in use

	// WorkFlow
	TOOL_CAA_PS_NAME("CAA - Peoplesoft"),
	TOOL_CAA_JDE_NAME("CAA - JDEdwards"), 
	TOOL_CAA_SAP_NAME("CAA - SAP"),
	
	TOOL_EDISCOVERY_NAME("e-Discovery"), //not in use
	TOOL_FFG_METALYZER_NAME("FFG Metalyzer"),
	TOOL_CAA_ORACLE_EBS_NAME("CAA - ORACLE EBS"), 
	;
	
	private String value;

	ToolName(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

}
