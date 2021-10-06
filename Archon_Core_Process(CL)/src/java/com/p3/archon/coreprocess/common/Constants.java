package com.p3.archon.coreprocess.common;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.logging.Level;

public class Constants {

	public static int MAX_TABLES_PARALLEL_EXT = 3;
	public static int MAX_SCHEMA_PARALLEL_EXT = 3;
	public static int MAX_RECORD_PER_XML_FILE = 100 * 1024 * 1024;

	public static boolean METADATA_COUNT = false;
	public static boolean SPLITDATE = false;
	public static boolean SHOW_LOB = false;
	public static boolean SHOW_DATETIME = false;
	public static boolean NEED_VIEWS = false;
	public static boolean NEED_TABLES = true;

	public static boolean IS_SIP_EXTRACT = false;

	public static Level LOG_LEVEL = Level.OFF;

	public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S");

	public static final String PROP_FILE = ".." + File.separator + "config" + File.separator + "variables.properties";
	public static ArrayList<String> ADDITIONAL_TABLE_TYPE;
	public static boolean MOVE_FILE_TO_NAS = false;
	public static String NAS_FILE_PATH;
	public static String TEMP_PATH;

}
