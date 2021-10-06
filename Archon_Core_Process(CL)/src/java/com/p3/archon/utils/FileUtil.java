package com.p3.archon.utils;

/*
 * $$HeadURL$$
 * $$Id$$
 *
 * CCopyright (c) 2015, P3Solutions . All Rights Reserved.
 * This code may not be used without the express written permission
 * of the copyright holder, P3Solutions.
 */
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;


/**
 * Class: FileUtil
 *
 *
 * This class is a File Util with basic File IO methods.
 *
 * @author Malik
 * @version 1.0
 *
 */
public class FileUtil {
	/**
	 * check if a directory exists
	 *
	 * @param fileDir
	 *            the file path
	 * @exception Exception
	 *                an error has occured
	 */
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

	/**
	 * check if a file exists
	 *
	 * @param fileDir
	 *            the file path
	 * @exception Exception
	 *                an error has occured
	 */
	public static boolean checkForFile(String fileDir) throws Exception {
		File f;
		try {
			// check for file existing
			f = new File(fileDir);
			return f.isFile();
		} finally {
			f = null;
		}
	}

	/**
	 * Gets the File name from a String Path. Example: if 'C:\\Program
	 * Files\\Test.doc' is passed in, 'Test.doc' is returned. Alsoe takes care of /
	 * slashes in the case of /Program Files/Test.doc'
	 *
	 * @param fullFileName
	 *            The file name to strip the file name from
	 *
	 * @return The file name
	 */
	public static String getFileNameFromPath(String fullFileName) {
		File f = new File(fullFileName);
		String fname = f.getName();
		f = null;
		return fname;
	}

	/**
	 * checks for and if does not exist - creates a directory
	 *
	 * @param fileDir
	 *            the file path
	 * @exception IOException
	 *                an error has occurred
	 */
	public static void checkCreateDirectory(String fileDir) throws Exception {
		System.out.println("CreateDirectory = " + fileDir);
		if (!checkForDirectory(fileDir))
			createDir(fileDir);
	}

	/**
	 * Create a directory based on parent path and name.
	 *
	 * @param dir
	 *            File of parent directory.
	 * @param name
	 *            Name of new directory.
	 * @return File
	 * @throws IOException
	 */
	public static File createDir(File dir, String name) throws IOException {
		return createDir(dir.getAbsolutePath() + File.separator + name);
	}

	/**
	 * Create a directory based on dir String passed in
	 *
	 * @param dir
	 *            File of parent directory.
	 * @return File
	 * @throws IOException
	 */
	public static File createDir(String dir, String name) throws IOException {
		return createDir(dir + File.separator + name);
	}

	/**
	 * Create a directory based on dir String passed in
	 *
	 * @param dir
	 *            File of parent directory.
	 * @return File
	 * @throws IOException
	 */
	public static File createDir(String dir) throws IOException {
		File tmpDir = new File(dir);
		if (!tmpDir.exists()) {
			if (!tmpDir.mkdirs()) {
				throw new IOException("Could not create temporary directory: " + tmpDir.getAbsolutePath());
			}
		} else {
		//	System.out.println("Not creating directory, " + dir + ", this directory already exists.");
		}
		return tmpDir;
	}

	/**
	 * Copy a file to another dir
	 *
	 * @param filetoMove
	 * @param destinationFilePath
	 *            Name of new directory.
	 *
	 */
	public static boolean movefile(String filetoMove, String destinationFilePath, boolean haltIfFail) {
		// File (or directory) to be moved
		File file = new File(filetoMove);

		// Destination directory
		File dir = new File(destinationFilePath);

		// Move file to new directory
		boolean success = file.renameTo(new File(dir, file.getName()));
		if (!success) {
			System.err.println("The file " + filetoMove + " was not successfully moved");
			if (haltIfFail)
				System.exit(1);
		}
		return success;
	}

	/**
	 * Delete the target directory and its contents.
	 *
	 * @param strTargetDir
	 *            Target directory to be deleted.
	 * @return <code>true</code> if all deletions successful, <code>false> otherwise
	 */
	public static boolean deleteDirectory(String strTargetDir) {
		File fTargetDir = new File(strTargetDir);
		if (fTargetDir.exists() && fTargetDir.isDirectory()) {
			return deleteDirectory(fTargetDir);
		} else {
			return false;
		}
	}

	/**
	 * Delete the target directory and its contents.
	 *
	 * @param dir
	 *            Target directory to be deleted.
	 * @return <code>true</code> if all deletions successful, <code>false> otherwise
	 */
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

	/**
	 * deleteFile
	 *
	 * @param filePath
	 *            the file path
	 * @exception Exception
	 *                an error has occured
	 */
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

	/**
	 * write file
	 *
	 * @param filePath
	 *            the file path
	 *
	 * @exception Exception
	 *                an error has occured
	 */
	public static void writeFile(String filePath, String txtToWrite) throws IOException {
		Writer out = new OutputStreamWriter(new FileOutputStream(filePath), "UTF-8");
		try {
			out.write(txtToWrite);
		} finally {
			out.close();
		}
	}

	public static void writeFileAppend(String filePath, String txtToWrite) throws IOException {
		Writer out = new OutputStreamWriter(new FileOutputStream(filePath, true), "UTF-8");
		try {
			out.write(txtToWrite);
		} finally {
			out.close();
		}
	}
}
