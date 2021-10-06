package com.p3.archon.core;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import com.p3.archon.dboperations.dbmodel.enums.CreationOption;
import com.p3.archon.securitymodule.SecurityModule;

public class ArchonDBUtils {
	private static final SessionFactory sessionFactory = buildSessionFactory();

	public static String ACF_FILE = "";
	public static final String HIB_CFG_FILE = "hibernate.cfg.xml";
	public static final String BASE_HIB_CFG_FILE = "hibernate0.cfg.xml";
	//public static final String USERS_HIB_CFG_FILE = "hibernate1.cfg.xml";

	private static String server;
	private static String port;
	private static String database;
	private static String userName;
	private static String password;
	private static String ssl;

	private static SessionFactory buildSessionFactory() {
		try {
			ACF_FILE = ".." + File.separator + "config" + File.separator + "config.acf";
			initialize();
			Configuration cfg = new Configuration().configure(BASE_HIB_CFG_FILE);
			String URL = "jdbc:mysql://" + server + ":" + port + "/" + database + "?createDatabaseIfNotExist=true&disableMariaDbDriver"
					+ ssl;
			String pwd = SecurityModule.perfromDecrypt(database, userName, password);
			cfg.configure();
			cfg.setProperty("hibernate.connection.url", URL);
			cfg.setProperty("hibernate.connection.username", userName);
			cfg.setProperty("hibernate.connection.password", pwd);
			return cfg.buildSessionFactory();
		} catch (Exception e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	private static void initialize() throws IOException {
		String text = IOUtils.toString(new FileReader(ACF_FILE));
		String[] line = SecurityModule
				.perfromDecrypt(CreationOption.ARCHON.toString(), SecurityModule.SEC_DEFAULT_KEY, text).split("\n");
		server = line[0];
		port = line[1];
		database = line[2];
		userName = line[3];
		password = line[4];
		ssl = line[5];
	}

	public static SessionFactory getSessionFactory() {
		return sessionFactory;
	}

	public static void closeSessionFactory() {
		sessionFactory.close();
	}
}