package com.p3.archon.dboperations.dbmodel;

import java.io.Serializable;
import java.sql.Blob;
import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "IACSB_SESSIONS", indexes = { @Index(name = "iacsb_sessions_index", columnList = "SESSION_ID"),
		@Index(name = "iacsb_sessions_index1", columnList = "APPLICATION_NAME"),
		@Index(name = "iacsb_sessions_index2", columnList = "USER_ID"),
		@Index(name = "iacsb_sessions_index3", columnList = "USER_GROUP") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class IacsbSessions implements Serializable {

	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(generator = "UUID")
	@GenericGenerator(name = "UUID", strategy = "org.hibernate.id.UUIDGenerator")
	@Column(name = "SESSION_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String sessonId;

	@Column(name = "APPLICATION_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String appName;
	
	@Column(name = "DATABASE_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String dbName;

	@Column(name = "SEARCH_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String searchName;

	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String userId;

	@Column(name = "USER_GROUP", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String group;

	@Column(name = "SEARCH_DESC", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String searchDesc;

	@Column(name = "SESSION_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String sessionName;

	@Column(name = "SERVER", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String server;

	@Column(name = "HOST", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String host;

	@Column(name = "PORT", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String port;

	@Column(name = "SCHEMA_NAME", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String schema;

	@Column(name = "DB", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String database;

	@Column(name = "SAVE_DATE", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp saveDate;

	@Column(name = "BLOB_0", nullable = false)
	Blob blob0; // Object

	public String getSessonId() {
		return sessonId;
	}

	public void setSessonId(String sessonId) {
		this.sessonId = sessonId;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getSearchName() {
		return searchName;
	}

	public void setSearchName(String searchName) {
		this.searchName = searchName;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getSessionName() {
		return sessionName;
	}

	public void setSessionName(String sessionName) {
		this.sessionName = sessionName;
	}

	public String getSearchDesc() {
		return searchDesc;
	}

	public void setSearchDesc(String searchDesc) {
		this.searchDesc = searchDesc;
	}

	public Timestamp getSaveDate() {
		return saveDate;
	}

	public void setSaveDate(Timestamp saveDate) {
		this.saveDate = saveDate;
	}

	public Blob getBlob0() {
		return blob0;
	}

	public void setBlob0(Blob blob0) {
		this.blob0 = blob0;
	}

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getDatabase() {
		return database;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	
	public void setDatabase(String database) {
		this.database = database;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}
}
