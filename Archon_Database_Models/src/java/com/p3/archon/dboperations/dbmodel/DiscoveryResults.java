package com.p3.archon.dboperations.dbmodel;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "DISCOVERY_RESULTS", indexes = { @Index(name = "ed_index", columnList = "USER_ID"),
		@Index(name = "ed_index1", columnList = "SERVER"), @Index(name = "ed_index2", columnList = "HOST"),
		@Index(name = "ed_index3", columnList = "PORT"), @Index(name = "ed_index4", columnList = "DB"),
		@Index(name = "ed_index5", columnList = "SCHEMA_NAME") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class DiscoveryResults {

	@Id
	@Column(name = "JOB_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String jobId;

	@Column(name = "ANALYSIS_UPDATE_DATE", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp analysisDate;

	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userId;

	@Column(name = "SERVER", nullable = true, columnDefinition = "varchar(255)")
	String server;

	@Column(name = "HOST", nullable = true, columnDefinition = "varchar(255)")
	String host;

	@Column(name = "PORT", nullable = true, columnDefinition = "varchar(255)")
	String port;

	@Column(name = "DB", nullable = true, columnDefinition = "varchar(255)")
	String database;

	@Column(name = "SCHEMA_NAME", nullable = true, columnDefinition = "varchar(255)")
	String schema;

	@Column(name = "DISCOVERY_PARAM", nullable = false, columnDefinition = "varchar(255)")
	String discoveryParam;

	@Column(name = "DISCOVERY_RESULT", nullable = true, columnDefinition = "json DEFAULT NULL")
	String discoveryResult;

	public String getDiscoveryParam() {
		return discoveryParam;
	}

	public void setDiscoveryParam(String discoveryParam) {
		this.discoveryParam = discoveryParam;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public Timestamp getAnalysisDate() {
		return analysisDate;
	}

	public void setAnalysisDate(Timestamp analysisDate) {
		this.analysisDate = analysisDate;
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

	public String getDiscoveryResult() {
		return discoveryResult;
	}

	public void setDiscoveryResult(String discoveryResult) {
		this.discoveryResult = discoveryResult;
	}

}
