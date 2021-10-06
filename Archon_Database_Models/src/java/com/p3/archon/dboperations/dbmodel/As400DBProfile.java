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
@Table(name = "AS400_DB_PROFILE", indexes = { @Index(name = "As400_db_profile_index", columnList = "DB_ID"),
		@Index(name = "As400_db_profile_index1", columnList = "USER_ID"),
		@Index(name = "As400_db_profile_index2", columnList = "HOST"),
		@Index(name = "As400_db_profile_index3", columnList = "PORT"),
		@Index(name = "As400_db_profile_index4", columnList = "LIB") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class As400DBProfile {

	@Id
	@GeneratedValue(generator = "UUID")
	@GenericGenerator(name = "UUID", strategy = "org.hibernate.id.UUIDGenerator")
	@Column(name = "DB_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String dbId;

	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userId;

	@Column(name = "HOST", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String host;

	@Column(name = "PORT", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String port;

	@Column(name = "PROFILE_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String profileName;

	@Column(name = "LIB", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String lib;

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

	public String getLib() {
		return lib;
	}

	public void setLib(String lib) {
		this.lib = lib;
	}

}
