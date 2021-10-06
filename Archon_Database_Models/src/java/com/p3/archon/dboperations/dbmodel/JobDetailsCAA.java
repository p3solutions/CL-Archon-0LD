package com.p3.archon.dboperations.dbmodel;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import com.p3.archon.dboperations.dbmodel.enums.CaaAppType;

@Entity
@Table(name = "JOB_DETAILS_CAA", indexes = { @Index(name = "job_details_caa_index", columnList = "JOB_ID"),
		@Index(name = "job_details_caa_index1", columnList = "USER_ID") })
public class JobDetailsCAA
{

	@Id
	@Column(name = "JOB_ID")
	String jobId;
	
	@Column(name = "JOB_NAME")
	String jobName;
	
	@Column(name = "USER_ID")
	String userId;
	
	@Column(name = "CREATED_DT")
	Timestamp createdDt;
	
	@Column(name = "SERVER")
	String server;
	
	@Column(name = "HOST")
	String host;
	
	@Column(name = "PORT")
	String port;
	
	@Column(name = "DATABASE_NAME")
	String database;
	
	@Column(name = "SCHEMA_NAME")
	String schema;
	
	@Column(name = "USER_NAME")
	String username;
	
	@Column(name = "USER_PASS")
	String password;
	
	@Column(name = "WF_INC")
	String wfInc;
	
	@Column(name = "SCRIPT1", length = 1000)
	String script1;
	
	@Column(name = "SCRIPT2", length = 1000)
	String script2;
	
	@Column(name = "SCRIPT3", length = 1000)
	String script3;
	
	@Column(name = "SCRIPT4", length = 1000)
	String script4;
	
	@Column(name = "SCRIPT5", length = 1000)
	String script5;
	
	@Column(name = "SCRIPT6", length = 1000)
	String script6;
	
	@Column(name = "SCRIPT7", length = 1000)
	String script7;
	
	@Column(name = "SCRIPT8", length = 1000)
	String script8;
	
	@Enumerated(EnumType.STRING)
	@Column(name="APP_TYPE")
	CaaAppType appType;
	

	public CaaAppType getAppType() {
		return appType;
	}

	public void setAppType(CaaAppType appType) {
		this.appType = appType;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public Timestamp getCreatedDt() {
		return createdDt;
	}

	public void setCreatedDt(Timestamp createdDt) {
		this.createdDt = createdDt;
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

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getWfInc() {
		return wfInc;
	}

	public void setWfInc(String wfInc) {
		this.wfInc = wfInc;
	}

	public String getScript1() {
		return script1;
	}

	public void setScript1(String script1) {
		this.script1 = script1;
	}

	public String getScript2() {
		return script2;
	}

	public void setScript2(String script2) {
		this.script2 = script2;
	}

	public String getScript3() {
		return script3;
	}

	public void setScript3(String script3) {
		this.script3 = script3;
	}

	public String getScript4() {
		return script4;
	}

	public void setScript4(String script4) {
		this.script4 = script4;
	}

	public String getScript5() {
		return script5;
	}

	public void setScript5(String script5) {
		this.script5 = script5;
	}

	public String getScript6() {
		return script6;
	}

	public void setScript6(String script6) {
		this.script6 = script6;
	}

	public String getScript7() {
		return script7;
	}

	public void setScript7(String script7) {
		this.script7 = script7;
	}

	public String getScript8() {
		return script8;
	}

	public void setScript8(String script8) {
		this.script8 = script8;
	}
	
}
