package com.p3.archon.dboperations.dbmodel;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "ODBC_PROFILE", indexes = { @Index(name = "odbc_db_profile_index", columnList = "DB_ID"),
		@Index(name = "odbc_db_profile_index1", columnList = "USER_ID"),
		@Index(name = "odbc_profile_index2", columnList = "SERVER"),
		@Index(name = "odbc_db_profile_index3", columnList = "DATA_SOURCE_NAME"),
		@Index(name = "odbc_db_profile_index4", columnList = "SCHEMA_NAME") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class ODBCProfile {

	@Id
	@GeneratedValue(generator = "UUID")
	@GenericGenerator(name = "UUID", strategy = "org.hibernate.id.UUIDGenerator")
	@Column(name = "DB_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String dbId;

	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userId;

	@Column(name = "DATA_SOURCE_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String dataSourceName;

	@Column(name = "PROFILE_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String profileName;

	@Column(name = "SERVER", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String server;

	@Column(name = "SCHEMA_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String schema;

	@Column(name = "USER_NAME", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String userName;

	@Column(name = "PASSWORD", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String userPwd;

	public String getDbId() {
		return dbId;
	}

	public void setDbId(String dbId) {
		this.dbId = dbId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getUserPwd() {
		return userPwd;
	}

	public void setUserPwd(String userPwd) {
		this.userPwd = userPwd;
	}

	public String getProfileName() {
		return profileName;
	}

	public void setProfileName(String profileName) {
		this.profileName = profileName;
	}

	public String getDataSourceName() {
		return dataSourceName;
	}

	public void setDataSourceName(String dataSourceName) {
		this.dataSourceName = dataSourceName;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

}
