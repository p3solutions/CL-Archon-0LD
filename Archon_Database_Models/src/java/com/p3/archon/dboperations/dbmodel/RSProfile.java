package com.p3.archon.dboperations.dbmodel;

import java.sql.Blob;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "RS_PROFILE", indexes = { @Index(name = "rs_profile_index", columnList = "RS_ID"),
		@Index(name = "rs_profile_index1", columnList = "USER_ID"),
		@Index(name = "rs_profile_index2", columnList = "CONNECTIONTYPE"),
		@Index(name = "rs_profile_index3", columnList = "HOST"),
		@Index(name = "rs_profile_index4", columnList = "PORT"),
		@Index(name = "rs_profile_index5", columnList = "PATH"),
		@Index(name = "rs_profile_index6", columnList = "REMOTEDIR") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class RSProfile {

	@Id
	@GeneratedValue(generator = "UUID")
	@GenericGenerator(name = "UUID", strategy = "org.hibernate.id.UUIDGenerator")
	@Column(name = "RS_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String rsId;

	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userId;

	@Column(name = "PROFILE_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String profileName;

	@Column(name = "CONNECTIONTYPE", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String connectionType;

	@Column(name = "HOST", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String host;

	@Column(name = "PORT", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String port;

	@Column(name = "PATH", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String path;

	@Column(name = "REMOTEDIR", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String remotedir;

	@Column(name = "USER_NAME", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String userName;

	@Column(name = "PASSWORD", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String userPwd;

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

	public String getRsId() {
		return rsId;
	}

	public void setRsId(String rsId) {
		this.rsId = rsId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getProfileName() {
		return profileName;
	}

	public void setProfileName(String profileName) {
		this.profileName = profileName;
	}

	public String getConnectionType() {
		return connectionType;
	}

	public void setConnectionType(String connectionType) {
		this.connectionType = connectionType;
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

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getRemotedir() {
		return remotedir;
	}

	public void setRemotedir(String remotedir) {
		this.remotedir = remotedir;
	}

}
