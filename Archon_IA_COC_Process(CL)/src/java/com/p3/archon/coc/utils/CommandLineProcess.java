package com.p3.archon.coc.utils;

/*
 * $$HeadURL$$
 * $$Id$$
 *
 * CCopyright (c) 2015, P3Solutions . All Rights Reserved.
 * This code may not be used without the express written permission
 * of the copyright holder, P3Solutions.
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.UUID;

/**
 * @author Malik
 * 
 *         General Utility to run a command line process
 *
 */
public class CommandLineProcess {

	public String outputLogFile;

	/**
	 * runs the command
	 * 
	 * @param command
	 *            the command
	 * 
	 * @return returns true if the script ran successfully
	 * @throws Exception
	 *             if there is an error
	 */
	public boolean run(String[] command, String appname) throws Exception {
		String checkLine = "cd applications/" + appname;
		return run(command, checkLine, null);
	}

	/**
	 * runs the command
	 * 
	 * @param command
	 *            the command
	 * @param fileDirectoryToRunCommand
	 *            the directory on the file system to where the command is to be run
	 * 
	 *
	 * @return returns true if the script ran successfully
	 * @throws Exception
	 *             if there is an error
	 */
	public boolean run(String[] command, String cmdcheckline, File fileDirectoryToRunCommand) throws Exception {
		try {
			// do this for debugging to print out the command to be run
			int len = command.length;
			String newCommand = "";
			String sep = "";
			for (int i = 0; i < len; i++) {
				newCommand += sep + command[i];
				sep = " ";
			}
			System.out.println("Running command =  ('" + newCommand + "')");

			// Now run the process
			Process p;
			if (fileDirectoryToRunCommand != null) {
				System.out.println(
						"Running command at File Directory = '" + fileDirectoryToRunCommand.getAbsolutePath() + "'");
				p = Runtime.getRuntime().exec(command, null, fileDirectoryToRunCommand);
			} else {
				p = Runtime.getRuntime().exec(command);
			}
			System.out.println("Process successfully created.");
			String uuid = UUID.randomUUID().toString();
			String errorlog = "report_" + uuid + ".log";
			String outputlog = "report_" + uuid + ".log";
			outputLogFile = new File(outputlog).getAbsolutePath();
			// any error or output messages
			CommandLineStream errorGobbler = new CommandLineStream(p.getErrorStream(), "ERROR", errorlog, p,
					cmdcheckline);
			CommandLineStream outputGobbler = new CommandLineStream(p.getInputStream(), "OUTPUT", outputlog, p,
					cmdcheckline);
			errorGobbler.start();
			outputGobbler.start();
			int exitVal = p.waitFor();
			if (exitVal != 0) {
				System.out.println("Return value error occurred. Value = " + exitVal);
				return false;
			}
			System.out.println("Return value of process is " + exitVal);
			return true;
		} catch (Exception e) {
			System.out.println("An Error has occurred = " + e.getMessage());
			throw new Exception("An Error has occurred: " + e.getMessage(), e);
		}

	}

	/**
	 * runs the command
	 * 
	 * @param command
	 *            the command
	 * @param fileDirectoryToRunCommand
	 *            the directory on the file system to where the command is to be run
	 * 
	 *
	 * @return returns true if the script ran successfully
	 * @throws Exception
	 *             if there is an error
	 */
	public boolean run(String command, String cmdcheckline, File fileDirectoryToRunCommand) throws Exception {
		try {
			// Now run the process
			Process p;
			if (fileDirectoryToRunCommand != null) {
				System.out.println(
						"Running command at File Directory = '" + fileDirectoryToRunCommand.getAbsolutePath() + "'");
				p = Runtime.getRuntime().exec(command, null, fileDirectoryToRunCommand);
			} else {
				p = Runtime.getRuntime().exec(command);
			}

			System.out.println("Process successfully created.");
			String uuid = UUID.randomUUID().toString();
			String errorlog = "report_" + uuid + ".log";
			String outputlog = "report_" + uuid + ".log";
			outputLogFile = new File(outputlog).getAbsolutePath();
			// any error or output messages
			CommandLineStream errorGobbler = new CommandLineStream(p.getErrorStream(), "ERROR", errorlog, p,
					cmdcheckline);
			CommandLineStream outputGobbler = new CommandLineStream(p.getInputStream(), "OUTPUT", outputlog, p,
					cmdcheckline);
			// kick off the threads
			errorGobbler.start();
			outputGobbler.start();
			int exitVal = p.waitFor();
			if (exitVal != 0) {
				System.out.println("Return value error occurred. Value = " + exitVal);
				return false;
			}
			System.out.println("Return value of process is " + exitVal);
			return true;
		} catch (Exception e) {
			System.out.println("An Error has occurred = " + e.getMessage());
			throw new Exception("An Error has occurred: " + e.getMessage(), e);
		}
	}

}

/**
 * @author Malik
 * 
 *         CommandLineStream - thread class to read the input and error message
 *         streams from the command line
 *
 */
class CommandLineStream extends Thread {
	InputStream is;
	String type;
	String file;
	Writer out;
	Process p;
	String checkline;
	boolean flag = false;

	public final static char CR = (char) 0x0D;
	public final static char LF = (char) 0x0A;

	public final static String CRLF = "" + CR + LF;

	CommandLineStream(InputStream is, String type, String file, Process p, String checkline) {
		this.is = is;
		this.type = type;
		this.file = file;
		this.p = p;
		this.checkline = checkline;
		try {
			this.out = new OutputStreamWriter(new FileOutputStream(file));
		} catch (Exception e) {
		}
	}

	public void run() {
		try {
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			String line = null;
			if (this.out != null) {
				while ((line = br.readLine()) != null) {
					if (line.contains("cd ../../")) {
						continue;
					}
					if (line.contains("command line client (c) OpenText Corporation")
							|| line.contains(
									"Type 'help' for a list of commands and options, type 'exit' to leave the shell.")
							|| line.contains("quit")) {
						continue;
					}
					if (checkline != null && line.contains(checkline)) {
						flag = true;
						continue;
					}
					if (line.startsWith("Connected to")) {
						flag = true;
					}
					if (flag && line.contains("End time:")) {
						this.out.write(CRLF);
						this.out.write(CRLF);
						this.out.write(CRLF);
						System.err.println("\nUnable to find the specified Application. Please check application name");
						this.out.write("\nPlease check application name.");
						this.out.write(CRLF);
						this.out.write(CRLF);
						this.out.write(CRLF);
						this.out.write(line);
						this.out.write(CRLF);
						flag = false;
						continue;
					} else if (flag) {
						flag = false;
					}
					if (line.contains("(Permission denied)")
							|| line.contains("Sorry, password you entered is incorrect, try again.")
							|| line.contains("Connection was not established. Try re-entering password.")) {
						this.out.write(CRLF);
						this.out.write(CRLF);
						this.out.write(CRLF);
						this.out.write(line);
						this.out.write(line.contains("(Permission denied)") ? "\nPermission unavailable"
								: "\nCredentials are invalid");
						this.out.write(CRLF);
						System.out.println(line.contains("(Permission denied)") ? "\nPermission unavailable"
								: "\nCredentials are invalid");
						System.err.println(line.contains("(Permission denied)") ? "\nPermission unavailable"
								: "\nCredentials are invalid");
						out.flush();
						out.close();
						p.destroy();
						return;
					}
					if (line.startsWith("connect")) {
						this.out.write(line.substring(0, line.indexOf("--p ") + 4));
						this.out.write(line.substring(line.indexOf("--p ") + 4).replaceAll(".", "*"));
						this.out.write(CRLF);
					} else if (line.startsWith("WARNING:")) {
						this.out.write(CRLF);
					} else {
						this.out.write(line);
						this.out.write(CRLF);
					}
				}
				out.flush();
				out.close();
			} else
				while ((line = br.readLine()) != null)
					System.out.println(type + ">" + line);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}