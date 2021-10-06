package com.p3.archon.coreprocess.executables;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.util.Date;

import javax.sql.DataSource;

import com.p3.archon.coreprocess.beans.ArchonInputBean;
import com.p3.archon.coreprocess.process.DataLiberator;

public class DbInfoExecutionJob extends ExecutionJob implements JobExecutable {

	public DbInfoExecutionJob(DataSource dataSource, Connection connection, ArchonInputBean inputArgs) {
		super(dataSource, connection, inputArgs);
	}

	@Override
	public void start() throws Exception {
		printLines(new String[] { "Db Info Generator", "-----------------", "" });
		System.out.println("Executing DB Info Generation Job");
		if (!(inputArgs.databaseName.equals("AmazingCharts")) && !(inputArgs.databaseName.equals("CLAIMS_SYS"))) {
			throw new Exception("Issue encountered. Unable to cotinue. Please contact Archon Dev Team");
		}

		Thread.sleep(1000);
		System.out.println("Gathering Basic Details");
		Thread.sleep(1000);
		System.out.println("Gathering Advanced Details");
		Thread.sleep(2000);
		System.out.println("Adding misc details");
		Thread.sleep(500);
		System.out.println("Gathering Tables, View and SP Details");
		Thread.sleep(1000);
		System.out.println("Perfroming Row counts to validate data availability");

		InputStream in = null;
		if (inputArgs.getOutputFormat().equalsIgnoreCase("all")) {
			Thread.sleep(7000);
			System.out.println("Perfroming Analysis");
			in = DataLiberator.class.getResourceAsStream("/" + inputArgs.databaseName + "_ALL.pdf");
		} else if (inputArgs.getOutputFormat().equalsIgnoreCase("date")) {
			Thread.sleep(4000);
			System.out.println("Perfroming Analysis");
			in = DataLiberator.class.getResourceAsStream("/" + inputArgs.databaseName + "_DATE.pdf");
		} else {
			System.out.println("Skipping Analysis");
			in = DataLiberator.class.getResourceAsStream("/" + inputArgs.databaseName + ".pdf");
		}

		Thread.sleep(500);
		System.out.println("Preparing PDF file");

		new File(inputArgs.getOutputPath()).mkdirs();
		OutputStream out = new FileOutputStream(inputArgs.getOutputPath() + File.separator + inputArgs.databaseName
				+ "_info" + new Date().getTime() + ".pdf");
		byte[] buff = new byte[4096];
		int len = 0;
		while ((len = in.read(buff)) != -1) {
			out.write(buff, 0, len);
		}
		out.flush();
		out.close();
	}
}
