package com.p3.archon.constants;

public enum ExtractionConstants {

	CONNECTION_TYPE("-ct","Connection Type"),
	DATABASESERVER("-dbs","Database Server"),
	HOST("-h","Host IP Address"),
	PORT("-l","Port No"),
	DATABASE("-d","Database Name"),
	SCHEMA("-s","Schema Name"),
	USERNAME("-u","User Name"),
	PASSWORD("-p","Password"),
	AUDITLEVEL("-a","Audit Level"),
	OUTPUT_FILE_FORMAT("-o","Output File Format"),
	OUTPUT_PATH("-op", "Output Path"),
	COMMAND("-c","Command"),
	RPX("-rpx","XML File Split Size (in MB)"),
	BPF("-rpx","Records per xml"),
	UUID("-uuid","Job reference Id"),

	//UDE
	EXTRACTION_TABLENAME("-utn","Extraction Table Name"), 
	BLOB_IDENTIFIER("-bi","Blob Identifier"),
	BLOB_COLUMN("-ubc","Blob Column Name"), 
	FILE_NAME_COLUMN("-ufc","Column containing File Name"),
	FILE_NAME_SEQ_START_NUMBER("-uss","File name seq start no"),
	FILE_NAME_PREFIX("-ufp","File name prefix"),
	FILE_NAME_OPTION("-ufo","File name option"),
	FILE_EXT_OPTION("-ueo","File extention option"),
	FILE_EXT_COLUMN("-uec","Column containing File Extention"),
	FILE_EXT_VALUE("-uev","File extention value"), 
	VERSION_INFO_OPTION("-uvo","Version info option"),
	ID_COLUMN("-uic","ID Column"),
	FILE_SEQ_COLUMN("-usc","File Seq Column"),
	VERSION_COLUMN("-uvc","Version Column"),


	
	//RDBMS
	TABLE_INCLUSION_REGEX("-t", "Table name/Table inclusion Regex"),
	INCLUDE_RELATIONSHIP("-r", "Include Relationship"), 
	MAX_PARALLEL_PROCESS("-mpp", "Maximum Parallel Process"), 
	METADATA_COUNT("-mc", "Include count in metadata file"),
	NEED_TABLES("-nt", "Need table in operation"),  
	NEED_VIEWS("-nv", "Need views in operation"), 
	SPLIT_DATE_FIELDS("-sdf", "Split date field for XML compatability"),
	EXTRACT_LOBS("-sl", "Extract lob as part of data"),
	SHOW_ORACLEDATEWITHTIME("-sodt", "Extract datetime as part of data for oracleserver"),
	QUERY_FILE("-qf", "Query is file"), 
	QUERY("-q", "Query"), 
	QUERY_TITLE("-qt", "Query title"),
	PASSWORD_ENCRYPTED("-e", "Password Encrypted"),
	INFOARCHIVE_VERSION("-v", "Infoarchive Version"),
	DOT_PATH("-gv", ""),
	EXT_SIP_IA_APP_NAME("","Sip Application Name"),
	EXT_SIP_HOLDING_PREFIX("","Holding Prefix"),
	
	//IAAA
	METADATA_FILE_LOCATION("metadataFileLocation", "Metadata File Location"),
	IA_APPLICATION_NAME("appName", "IA Application Name"), 
	IA_DATABASE_NAME("dbName", "IA Database Name"),
	CREATE_SEARCHES("createsearches","Create Searches"),
	SAVE_LOCATION("saveLoc", "Save Location"),
	APPLICATION_CATEGORY("category", "Application Category"),
	APPLICATION_DESCRIPTION("description", "Application description"),
	ENCRYPT_STRUCTURED_DATA("encryptStructureData", "Encrypt Structured Data"),
	ENCRYPT_UNSTRUCTURED_DATA("encryptUnstructureData", "Encrypt Unstructured Data"),
	EXCLUDE_EMPTY_TABLES("excludeZeroTables", "Exclude Screens for empty tables"), 
	IS_METADTA_BROWSED("Is meta browsed","-NA-"),
	PROFILE_NAME("Profile Name","-NA-"),
	ORIGINAL_META_LOC("Original Meta Loc","-NA-"),
	SEARCH_STATUS("", "Search Status"), 
	IA_USERNAME("-u", "Infoarchive Username"), 
	IA_USER_PASSWORD("-p", "Infoarchive User Password"),
	IAAA_CODE_SELECTIONS("", "Screen/Report Codes Selected"),
	IAAA_DIVISION("", "App Specification"),
	IAAA_MODULE("", "Module Specification"),
	
	//DOCUMENTUM
	AUTO_CREATE_APP("autoAppCreate", "Auto create Application"), 
	SIPS("data", "SIP Application"), 
	DOCBASE("docBase", "Doc Base"), 
	SYSOBJECTTYPE("sysObjectType", "Sys Object Type"), 
	HOLDING("holding", "Holding Name"), 
	SPLIT_SIZE("splitSize", "SIP file split size (in MB)"),
	FOLDER_INCLUDE_LIST("folderIncludeList", "Folders to include"), 
	QUERY_MODE("-qm", "Query Mode"),
	VALIDATION_MODE("", "Validation Mode"), 
	EXTRACT_TYPES("","Extract Types"), 
	EXTRACT_RELATIONS("","Extract Relations"), 
	EXTRACT_CONTENTS("","Extract Contents"),
	ENABLE_LARGE_XML_PROCESSING("","Enable Large XML Processing"), 
	INCLUDE_PID_HASH_IN_SIP("","Include PID Hash in SIP"), 
	EXTRACT_FOLDERS("","Extract Folders"), 
	EXTRACT_ACL("","Extract ACLs"),
	EXTRACT_CONTAINMENTS("","Extract Containments"),
	
	//FILESHARE
	FILESHARE_INPUT_PATH("inputPath", "Input dir path"), 
	PRODUCER("producer", "Producer"), 
	ENTITY("entity", "Entity"), 
	FILE_EXTENTION_LIST("fileExtentionList", "File extentions"), 
	GENERATE_METADATA("generatemetadata", "Genarate Metadata"), 
	EXTRACT_COLUMN_LIST("colList", "Extraction column list"), 
	INCLUDE_EXTENTIONS("includeExtentions", "Include Extentions"), 
	PROCESS_FILE_TO_REMOTE("pft", "Process File to Remote"), 

	//LOTUS NOTES
	INCLUDE_FORM_LIST("forms", "Forms included in extraction"),
	MAIL_DATBASE("-md", "Is Mail Database"),
	EXCEL_MODE("", "Inputs from Excel sheet"),
	
	//SHAREPOINT
	PROTOCOL("protocol", "Protocol"), 
	HOST_URL("hostUrl", "Host Url"), 
	GENERATE_REPORT("report", "Generate Report"), 
	RELATIVE_PATH_LIST("relativePathList", "Relative Path List"), 
	IS_INCLUDE_RELATIVE_PATH("isIncludePath", "Include/Exclude Relative Path in Extraction"), 
	SHAREPOINT_CONFIG_FILE("configFile", "Sample.config"), 
	SHAREPOINT_PUBLICKEY_FILE("publicKey", "public.xml"), 
	SHAREPOINT_PRIVATEKEY_FILE("privateKey", "private.xml"), 
	KEY_BASE_DIR("keyBaseDir", "encryptionKeysFolder"),
	IS_EXTRACT_ANALYSIS_REPORT("", "Extract Analysis Report"), 
	IS_EXTRACT_SUB_SITES("", "Extract Subsites Recursively"), 
	IS_EXTRACT_SUB_DIRECTORIES("", "Extract Subdirectories Recursively"),
	
	// CSB
	CSB_APP_NAME("IA App Name","IA Application Name"), 
	CSB_DB_NAME("IA Database Name","IA Database Name"), 
	CSB_SCREEN_NAME("Search Name","Search Name"), 
	CSB_SCREEN_DESC("Search Description","Search Description"), 
	CSB_MAIN_TABLE("Search Main Table","Search Main Table"), 
	CSB_NESTED_SEARCHES("Nested Searches Link","Nested Searches Link"),
	
	// Metalyzer
	USER_ID("-ui","User ID"),
	SESSION_ID("-si", "Session Id"),
	SCHEMA_MODE("-sm","Include Schema Mode Analysis"),
	COLUMN_MODE("-cm","Include Column Mode Analysis"),
	SPV_MODE("-spv","Include SPV Mode Analysis"),
	MA_ERT_COMMAND("Execution Command","Execution Command"),
	MA_SELECTED_TABLE("Metadata generating table","Metadata generating table"),
	SAMPLE_RATE("-n","Sampling Rate"),
	MAIN_TABLE("-t","Main Table"),
	MAIN_TABLE_COLUMN("-tc","Main Table's Selected Column"),
	META_ACTIVITY("","Activity"),
	DC_COUNT("-c","-NA-"),
	
	//ERT
	JOB_ID("-jid","Job Id"),
	IS_EXTRACT_JOB("","Extraction Job"),
	TABLE_LIST("-tl", "Table List"),
	SDR_COMMAND("Metadata Generation", "Pdi Schema Generation"), 
	WF_ID("-wi","Workflow Id"), 
	WF_NAME("","Workflow Name"),
	ERT_EXTRACTION("","Extraction"), 
	JOB_NAME("-jn","Job Name"),
	
	// As400 Extractor
	AS400_METADATA_COUNT("-tc", "Include count in metadata file"),
	AS400_ACTIVITY_LIST("-aot", "Limit table activities list"),
	LIBRARY("-s", "Library"),
	AS400_LIBRARY("-d", "Library/Database"),
	AS400_TABLE_INCLUSION_REGEX("-t", "File/Table inclusion Regex"),
	AS400_OPTIONS("-opt", "-NA-"),
	AS400_TIME_FORMAT("-tf", "Time Format"),
	
	// AS400 Metalyzer
	RERUN("-r", "Re-create Base session"), 
	TABLE_DETAIL("-td", "Table Detail"),
	
	// COC
	IS_COC("", "Chain of Custody"),
	METAFILES("-mf","Metadata File(s) path"),

	//XML_FILE
	CONFIG_DIR("", "Config Directory Path"),
	DATA_DIR("", "Data Directory Path"),
	NOTIFICATION_FILE("", "Notification File"), 
	RESPONSE_DIR("", "Response Directory Path"),
	DATA_FILENAME_INDICATOR("", "Data Filename Indicator"),
	
	// ING
	DATA_PATH("-dp", "InfoArchive Tables Path"),
	APP_TYPE("-at", "Applicaiton Type Table/Sip"), 
	INGEST_DATA("-id", "Ingest Data"), 
	INGEST_METADATA("-meta", "Ingest Metadata"), 
	ERROR_LOG_PATH("-el", "Error log file path"),
	OUTPUT_LOG_PATH("-ol", "Output log file path"), 
	ARCHON_INSTALLATION_PATH("-aip", "Archon installation path"), 
	SUMMARY_REPORT("-sr", "Summary Report"), 
	ARCHON_BIN_DIR("-bin", "Archon bin Dir"),
	INGEST_APP("-iapp","Ingest Application"),
	
	//E-DISCOVERY
	DEFINITION("", "Definition"), 
	DESCRIPTION("", "Description"), 
	DISCOVERY_PROFILE("", "DiscoveryProfile"), 
	TYPE("", "Type"),
	
	//Peoplesoft
	CATEGORY("-category","Category"),
	SUBCATEGORY("-subcategory","SubCategory"),
	
	//FLAT FILE GENERATOR
	INPUT_PATH("-i", "Input Directory"),
	DELIMITER("-d", "Delimiter"),
	HEADER("-h", "Header Row Present in the File"),
	COLUMN_HEADER("-chl", "Header Info"),
	EXTENTION("-e", "File Extention"),
	TABLE_NAME("-t", "Table Name"),
	TABLE_COLUMN_NAME("-tc", "Column List"),
	USER_COLUMN_NAME("-tc", "User Column"),
	TABLE("-tab", "Table"),
	SIP("-sip", "Sip"),
	SCHEMADETAILS("-schema", "Schem Details"),
	COLUMN_NAME("-colname", "Column Name"),
	COLUMN_TYPE("-coltype", "Column Type"),
	COLUMN_CONFIGURATION("-colconfig", "Column Configuration"),
	COLUMN_SKIPPED("-cs", "Column Skipped"),
	METADATA("-m", "Header row from Metadata"),
	DATE_FORMAT("-dtf", "Date Format"),
	TIME_FORMAT("-tf", "Time Format"),
	DATETIME_CONTAINS_SPACE("-dtcs", "DateTime Contains Space"),
	POST_PROCESSING_FILES("-p", "Post Processing Files"),
	ZIP_FILE_TYPE("-zft","Zip File Type"),
	BLOB_CONTENT_INPUT_FORMAT("-bif","Blob Content Input Format"),

	
	// CAA
	CAA_CODES("-code","Screen Codes Selected"),
	CAA_SESSION_ID("-rid","Record id"),
	CAA_PERFROMR_EXT("-cpe","Perform Extraction Activity"),
	CAA_PERFORM_SCR("-cps","Perform Screen Build Activity"),
	CAA_APP_NAME("-can","IA Application Name to build Screen"),
	CAA_DB_NAME("-cdn","IA Database Name to build Screen"),
	CAA_PHASE("-ph", "State"),
	CAA_PERFROMR_EXT_RC("-cperc","Extraction Required Columns Only"),
	
	//jobs	
	JOB_ATTEMPT("-ja", "Job Attempt"), 
	RUN_ID("-rid", "Run Id"),
	
	// PEOPLESOFT METADATA
	USER_DEFINED_QUERRY("-udc","User Defined Query"),
	QUERY_DATA("-qd","Query Data"),
	INDEX_QUERY_DATA("-iqd","Index Query Data"),
	RECORD_COUNT("-rc","Record Count"),
	DB_TYPE("-dbt","Database Type"),
	SELECTED_TABLE("-st","Table Selected"),
	DEPLOY_APPLICATION("-da", "Deploy Application"),
	;
	
	private String key;
	private String value;
	
	private ExtractionConstants(String key, String value) {
		this.key = key;
		this.value = value;
	}
	
	public String getKey(){
		return key;
	}
	
	public String getValue(){
		return value;
	}

}
