package com.p3.archon.commonfunctions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

import com.p3.archon.commonutilities.PropReader;
import com.p3.archon.constants.CommonSharedConstants;
import com.p3.archon.dboperations.dbmodel.JobDetails;

public class CommonFunctions {

	public static String checkCreateOutputSaveLocation(JobDetails jd) {
		if (CommonSharedConstants.OUTPUT_BASE_PATH.equals(""))
			setOutputPath();
		String outputPath = CommonSharedConstants.OUTPUT_BASE_PATH;
		try {
			while (outputPath.endsWith("\\") || outputPath.endsWith("/")) {
				outputPath = outputPath.substring(0, outputPath.length() - 1);
			}
			outputPath = outputPath + File.separator + jd.getUserId().replace(" ", "_") + File.separator
					+ formatPath(jd.getToolName().getValue()) + File.separator
					+ CommonSharedConstants.CCYYMMDD_FORMATTER.format(new Date()) + File.separator
					+ jd.getJobName().replace(" ", "_") + "-" + jd.getRowId().getJobId() + "_"
					+ jd.getRowId().getRunId() + "_" + jd.getRowId().getJobAttempt();
			checkCreateDirectory(outputPath);
		} catch (Exception e) {
			jd.setMessage(e.getMessage());
			e.printStackTrace();
		}
		return outputPath;
	}

	public static String checkCreateFSOutputSaveLocation(JobDetails jd, String op) {
		String outputPath = op;
		try {
			while (outputPath.endsWith("\\") || outputPath.endsWith("/")) {
				outputPath = outputPath.substring(0, outputPath.length() - 1);
			}
			outputPath = outputPath + File.separator + jd.getUserId().replace(" ", "_") + File.separator
					+ formatPath(jd.getToolName().getValue()) + File.separator
					+ CommonSharedConstants.CCYYMMDD_FORMATTER.format(new Date()) + File.separator
					+ jd.getJobName().replace(" ", "_") + "-" + jd.getRowId().getJobId() + "_"
					+ jd.getRowId().getRunId() + "_" + jd.getRowId().getJobAttempt();
			checkCreateDirectory(outputPath);
		} catch (Exception e) {
			jd.setMessage(e.getMessage());
			e.printStackTrace();
		}
		return outputPath;
	}

	public static void getIaVersion() {
		try {
			PropReader p = new PropReader(CommonSharedConstants.PROP_FILE);
			CommonSharedConstants.IA_VERSION = p.getStringValue("IA_VERSION", "");
		} catch (Exception e) {
			CommonSharedConstants.IA_VERSION = "16EP6";
		}
		CommonSharedConstants.IA_VERSION = CommonSharedConstants.IA_VERSION.isEmpty() ? ""
				: CommonSharedConstants.IA_VERSION.trim();
	}

	public static String getIaVersionTemp() {
		CommonFunctions.readUpdatePropFile();
		String iaPath = CommonSharedConstants.INFOARCHIVE_INSTALLATION_PATH;
		iaPath += File.separator + "examples" + File.separator + "legacy-ant-applications";

		if (new File(CommonSharedConstants.INFOARCHIVE_INSTALLATION_PATH + File.separator + "version.txt").exists()) {
			File file = new File(CommonSharedConstants.INFOARCHIVE_INSTALLATION_PATH + File.separator + "version.txt");
			FileReader fr = null;
			BufferedReader br = null;
			try {
				fr = new FileReader(file);
				br = new BufferedReader(fr);
				String line;
				while ((line = br.readLine()) != null) {
					return line.split(":")[1].trim().substring(0, 4);
				}
			} catch (IOException e) {
				return "20.2";
			} finally {
				try {
					br.close();
					fr.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return "20.2";
		} else if (new File(CommonSharedConstants.INFOARCHIVE_INSTALLATION_PATH + File.separator + "config-default")
				.exists())
			return "16EP6";
		else if (!new File(iaPath).exists())
			return "16EP5";
		else
			return "16EP4";
	}

	public static void readUpdatePropFile() {
		PropReader p = new PropReader(CommonSharedConstants.PROP_FILE);
		CommonSharedConstants.DOT_CMD = p.getStringValue("DOT_PATH", "dot");
		CommonSharedConstants.ARCHON_INSTALLATION_PATH = p.getStringValue("ARCHON_PATH", "");
		CommonSharedConstants.INFOARCHIVE_INSTALLATION_PATH = p.getStringValue("INFOARCHIVE_PATH", "");
		CommonSharedConstants.OUTPUT_BASE_PATH = p.getStringValue("OUTPUT_BASE_PATH", "");
		CommonSharedConstants.IA_VERSION = p.getStringValue("IA_VERSION", "");
		CommonSharedConstants.IA_VERSION = CommonSharedConstants.IA_VERSION.isEmpty() ? ""
				: CommonSharedConstants.IA_VERSION.trim();
		CommonSharedConstants.TTF_CHECK = p.getBooleanValue("TTF", false);
		readUpdateTempPathPropFile();
		readUpdateJavaPathPropFile();
	}

	public static void readUpdateJavaPathPropFile() {
		try {
			PropReader p = new PropReader(CommonSharedConstants.PROP_FILE);
			String javaPath = p.getStringValue("JAVA_PATH");
			if (javaPath.equals("")) {
				throw new Exception("JAVA_PATH property is unavailable or undefined.");
			} else
				CommonSharedConstants.JAVA_PATH = "\"" + p.getStringValue("JAVA_PATH") + "\"";
		} catch (Exception e) {
			System.out.println("Setting up default JAVA_HOME location");
			CommonSharedConstants.JAVA_PATH = "java";
			e.printStackTrace();
		}
	}

	public static void readUpdateTempPathPropFile() {
		try {
			PropReader p = new PropReader(CommonSharedConstants.PROP_FILE);
			File file = null;
			CommonSharedConstants.TEMP_PATH = p.getStringValue("TEMP_PATH");
			if (CommonSharedConstants.TEMP_PATH.equals("")) {
//				System.out.println("Setting up default TEMP location");
				CommonSharedConstants.TEMP_PATH = System.getProperty("java.io.tmpdir");
			}
			file = new File(CommonSharedConstants.TEMP_PATH);
			file.mkdirs();
			if (!file.exists())
				throw new Exception("Unable to create dir in temp path mentioned in variable.properties.");

			File checkfile = new File(file.getAbsolutePath() + File.separator + "tempFile" + new Date().getTime());
			checkfile.createNewFile();
			if (checkfile != null && checkfile.isFile())
				checkfile.delete();
			CommonSharedConstants.TEMP_PATH = file.getAbsolutePath();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (CommonSharedConstants.TEMP_PATH == null || CommonSharedConstants.TEMP_PATH.equals("")) {
			CommonSharedConstants.TEMP_PATH = new File("").getAbsolutePath();
		}
	}

	private static void setOutputPath() {
		PropReader p = new PropReader(CommonSharedConstants.PROP_FILE);
		CommonSharedConstants.OUTPUT_BASE_PATH = p.getStringValue("OUTPUT_BASE_PATH", "");
		if (CommonSharedConstants.OUTPUT_BASE_PATH.equals(""))
			CommonSharedConstants.OUTPUT_BASE_PATH = p.getStringValue("ARCHON_PATH", "") + File.separator + "outputs";
	}

	public static void checkCreateDirectory(String fileDir) throws IOException, Exception {
//		System.out.println("CreateDirectory = " + fileDir);
		if (!checkForDirectory(fileDir))
			createDir(fileDir);
	}

	private static String formatPath(String value) {
		return value.replace(" ", "_");
	}

	public static File createDir(String dir) throws IOException {
		File tmpDir = new File(dir);
		if (!tmpDir.exists()) {
			if (!tmpDir.mkdirs()) {
				throw new IOException("Could not create temporary directory: " + tmpDir.getAbsolutePath());
			}
		} else {
			System.out.println("Not creating directory, " + dir + ", this directory already exists.");
		}
		return tmpDir;
	}

	public static boolean checkForDirectory(String fileDir) throws Exception {
		File f;
		try {
			// check for file existing
			f = new File(fileDir);
			return f.isDirectory();
		} finally {
			f = null;
		}
	}

	static String TEMP_FILE_SESSION = CommonSharedConstants.CCYYMMDD_FORMATTER.format(new Date()) + File.separator
			+ UUID.randomUUID().toString();

	public static void deleteTempDir() {
		try {
			deleteDirectory(createTempFolderMain());
			deleteDirectory(createTempJobScheuleFolder());
		} catch (Exception e) {

		}
	}

	public static String createTempJobScheuleFolder(String jobid) {
		String path = getTmpDir() + CommonSharedConstants.ARCHON_JOB + File.separator + TEMP_FILE_SESSION
				+ File.separator + "mixed" + File.separator + jobid;
		path = path.replace(" ", "_");
		try {
			checkCreateDirectory(path);
			path = path + File.separator;
		} catch (Exception e) {
			path = "";
		}
		path = new File(path).getAbsolutePath();
		path = path.endsWith(File.separator) ? path : path + File.separator;
		return path;
	}

	private static void checksetTempPath() {
		readUpdateTempPathPropFile();
	}

	private static String getTmpDir() {
		checksetTempPath();
		// String tmpDir = System.getProperty("java.io.tmpdir");
		String tmpDir = CommonSharedConstants.TEMP_PATH;
		tmpDir = tmpDir.endsWith(File.separator) ? tmpDir : tmpDir + File.separator;
		return tmpDir;
	}

	public static String createTempFolderMain() {
		String path = getTmpDir() + CommonSharedConstants.ARCHON + File.separator + TEMP_FILE_SESSION;
		path = path.replace(" ", "_");
		try {
			checkCreateDirectory(path);
			path = path + File.separator;
		} catch (Exception e) {
			path = "";
		}
		path = new File(path).getAbsolutePath();
		path = path.endsWith(File.separator) ? path : path + File.separator;
		return path;
	}

	public static String createTempJobScheuleFolder() {
		String path = getTmpDir() + CommonSharedConstants.ARCHON_JOB + File.separator + TEMP_FILE_SESSION;
		path = path.replace(" ", "_");
		try {
			checkCreateDirectory(path);
			path = path + File.separator;
		} catch (Exception e) {
			path = "";
		}
		path = new File(path).getAbsolutePath();
		path = path.endsWith(File.separator) ? path : path + File.separator;
		return path;
	}

	public static String createTempFolder() {
		String path = getTmpDir() + CommonSharedConstants.ARCHON + File.separator + TEMP_FILE_SESSION + File.separator
				+ "mixed";
		path = path.replace(" ", "_");
		try {
			checkCreateDirectory(path);
			path = path + File.separator;
		} catch (Exception e) {
			path = "";
		}
		path = new File(path).getAbsolutePath();
		path = path.endsWith(File.separator) ? path : path + File.separator;
		return path;
	}

	public static boolean deleteDirectory(String strTargetDir) {
		File fTargetDir = new File(strTargetDir);
		if (fTargetDir.exists() && fTargetDir.isDirectory()) {
			return deleteDirectory(fTargetDir);
		} else {
			return false;
		}
	}

	public static boolean deleteDirectory(File dir) {
		if (dir == null)
			return true;
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (String element : children) {
				boolean success = deleteDirectory(new File(dir, element));
				if (!success) {
					System.err.println("Unable to delete file: " + new File(dir, element));
					return false;
				}
			}
		}

		// The directory is now empty so delete it
		return dir.delete();
	}

	public static void deleteFile(String filePath) {
		File f;
		try {
			// check for file existing
			f = new File(filePath);
			if (f.isFile()) {
				f.delete();
			}
		} finally {
			f = null;
		}
	}

	public static String getTimeZone() {
		Calendar now = Calendar.getInstance();
		TimeZone timeZone = now.getTimeZone();
		switch (timeZone.getDisplayName()) {
		case "India Standard Time":
			return "IST";
		case "Coordinated Universal Time":
			return "UTC";
		default:
			return "UTC";
		}

	}
}
