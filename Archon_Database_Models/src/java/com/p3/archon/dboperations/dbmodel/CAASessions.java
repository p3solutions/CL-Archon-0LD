package com.p3.archon.dboperations.dbmodel;

import java.sql.Blob;
import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import com.p3.archon.dboperations.dbmodel.enums.CAAState;
import com.p3.archon.dboperations.dbmodel.enums.CaaAppType;
import com.p3.archon.dboperations.dbmodel.enums.JobStatus;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "CAA_WF_SESSIONS", indexes = { @Index(name = "caa_wf_index", columnList = "USER_ID"),
		@Index(name = "caa_wf_index1", columnList = "SERVER"), @Index(name = "caa_wf_index2", columnList = "HOST"),
		@Index(name = "caa_wf_index3", columnList = "PORT"), @Index(name = "caa_wf_index4", columnList = "DB"),
		@Index(name = "caa_wf_index5", columnList = "SCHEMA_NAME") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class CAASessions {

	@Id
	@Column(name = "SESSION_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String sessionId;

	@Column(name = "SERVER", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String server;

	@Column(name = "HOST", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String host;

	@Column(name = "PORT", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String port;

	@Column(name = "DB", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String database;

	@Column(name = "SCHEMA_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String schema;

	@Column(name = "USER_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String user;

	@Column(name = "PASSWORD", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String pass;

	@Column(name = "APP_TYPE", nullable = false, columnDefinition = "varchar(25) NOT NULL")
	CaaAppType appType;

	@Column(name = "CODE_LIST", nullable = false, columnDefinition = "varchar(4000) NOT NULL")
	String codeList;

	@Column(name = "CREATED_DATE", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp createdDate;

	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userId;

	@Column(name = "PROFILE_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String profileName;

	@Column(name = "STATE", nullable = false)
	CAAState state;

	@Column(name = "STATUS", nullable = false)
	JobStatus status;

	@Column(name = "ERROR", nullable = true, columnDefinition = "varchar(1000)")
	String error;

	@Column(name = "BLOB_1", nullable = true)
	Blob blob1;

	@Column(name = "BLOB_2", nullable = true)
	Blob blob2;

	@Column(name = "PROGRESS_BAR", nullable = false, columnDefinition = "int(6)")
	float progressBar;

	@Column(name = "UPDATED_DATE", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp updatedDate;

	@Column(name = "UPDATED_BY", nullable = true, columnDefinition = "varchar(255) NOT NULL")
	String updatedBy;

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
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

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPass() {
		return pass;
	}

	public void setPass(String pass) {
		this.pass = pass;
	}

	public String getUpdatedBy() {
		return updatedBy;
	}

	public void setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
	}

	public CAAState getState() {
		return state;
	}

	public void setState(CAAState state) {
		this.state = state;
	}

	public JobStatus getStatus() {
		return status;
	}

	public void setStatus(JobStatus status) {
		this.status = status;
	}

	public Timestamp getUpdatedDate() {
		return updatedDate;
	}

	public void setUpdatedDate(Timestamp updatedDate) {
		this.updatedDate = updatedDate;
	}

	public Timestamp getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Timestamp createdDate) {
		this.createdDate = createdDate;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public Blob getBlob1() {
		return blob1;
	}

	public void setBlob1(Blob blob1) {
		this.blob1 = blob1;
	}

	public Blob getBlob2() {
		return blob2;
	}

	public void setBlob2(Blob blob2) {
		this.blob2 = blob2;
	}

	public String getProfileName() {
		return profileName;
	}

	public void setProfileName(String profileName) {
		this.profileName = profileName;
	}

	public float getProgressBar() {
		return progressBar;
	}

	public void setProgressBar(float progressBar) {
		this.progressBar = progressBar;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public CaaAppType getAppType() {
		return appType;
	}

	public void setAppType(CaaAppType appType) {
		this.appType = appType;
	}

	public String getCodeList() {
		return codeList;
	}

	public void setCodeList(String codeList) {
		this.codeList = codeList;
	}

}
