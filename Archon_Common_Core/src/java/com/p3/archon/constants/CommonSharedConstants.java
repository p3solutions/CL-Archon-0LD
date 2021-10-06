package com.p3.archon.constants;

import java.io.File;
import java.text.SimpleDateFormat;

public class CommonSharedConstants {

	public static final SimpleDateFormat CCYYMMDD_FORMATTER = new SimpleDateFormat("yyyyMMdd");
	public static final String AOP_CONSTANT = "OUTPUT_BASE_PATH";
	public static final String IA_VERSION_KEY = "IA_VERSION";
	public static final String ARCHON_JOB = ".AJ";
	public static final String ARCHON = "ARCHON";
	public static final String BIN_DIR = "bin";
	public static final String ARCHON_JOBS_FOLDER = "jobs";

	public static String IA_VERSION = "16EP4";
	public static String OUTPUT_BASE_PATH = "";
	public static String PROP_FILE = ".." + File.separator + "config" + File.separator + "variables.properties";
	public static String ARCHON_INSTALLATION_PATH = "";
	public static String INFOARCHIVE_INSTALLATION_PATH = "";

	public static final String PREFIX_SAC = "sac";
	public static final String PREFIX_DOCUMENTUM = "dcm";
	public static final String PREFIX_UDE = "ude";
	public static final String PREFIX_SHAREPOINT = "sp";
	public static final String PREFIX_RDBMS = "rms";
	public static final String PREFIX_METALYZER = "ma";
	public static final String PREFIX_LOTUS_NOTES = "ln";
	public static final String PREFIX_APP_AUTOMATER = "aa";
	public static final String PREFIX_FILESHARE = "fs";
	public static final String PREFIX_ERT_ANALYSIS = "ea";
	public static final String PREFIX_AS400 = "as4";
	public static final String PREFIX_DC = "dc";
	public static final String PREFIX_ERT = "ert";
	public static final String SESSION_ID = "System_gen_";
	public static final String PREFIX_AS400_METALYZER = "as400_ma";
	public static final String PREFIX_AS400DC = "as400_dc";
	public static final String PREFIX_APP_CHAIN_OF_CUSTODY = "coc";
	public static final String PREFIX_XML_FILE_EXTRACTOR = "xf";
	public static final String PREFIX_APP_INGESTER = "ing";
	public static final String PREFIX_PEOPLESOFT = "pa";
	public static final String PREFIX_JDE = "jde";
	public static final String PREFIX_FLAT_FILE_GENERATOR = "ffg";
	public static final String PREFIX_CAA = "caa";
	
	public static final String TEMP_QUERY_FILE = "tempQuery.sql";
	public static String DOT_CMD = "dot"; // s"/opt/local/bin/dot";
	public static boolean TTF_CHECK = false;
	public static String TEMP_PATH = "";
	public static String JAVA_PATH = "java";
}

