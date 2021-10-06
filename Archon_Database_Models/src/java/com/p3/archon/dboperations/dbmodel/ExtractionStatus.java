package com.p3.archon.dboperations.dbmodel;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "EXTRACTION_STATUS", indexes = { @Index(name = "extraction_status_index1", columnList = "JOB_ID"),
		@Index(name = "extraction_status_index2", columnList = "RUN_ID"),
		@Index(name = "extraction_status_index3", columnList = "JOB_ATTEMPT"),
		@Index(name = "extraction_status_index4", columnList = "REQ_ID") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class ExtractionStatus {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "REQ_ID", unique = true, nullable = false)
	int reqId;

	@Column(name = "JOB_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String jobId;

	@Column(name = "RUN_ID", nullable = false, columnDefinition = "int(10) DEFAULT 1")
	private Integer runId;

	@Column(name = "JOB_ATTEMPT", nullable = false, columnDefinition = "int(10) DEFAULT 0")
	private Integer jobAttempt;

	@Column(name = "TABLE_NAME", nullable = true, columnDefinition = "varchar(255) NULL")
	String tableName;

	@Column(name = "END_TIME", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Date endTime;

	@Column(name = "START_TIME", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Date startTime;

	@Column(name = "COUNT_MATCH", nullable = true, columnDefinition = "varchar(10) NULL")
	String countMatch;

	@Column(name = "DESTINATION_RECORD", nullable = true, columnDefinition = "int NULL")
	int destRecord;

	@Column(name = "SOURCE_RECORD", nullable = true, columnDefinition = "varchar(255) NULL")
	String sourceRecord;

	@Column(name = "QUERY", nullable = true, columnDefinition = "varchar(12000) NULL")
	String query;

	@Column(name = "BLOBINFO", nullable = true, columnDefinition = "varchar(2500) NULL")
	String blobinfo;

	public String getBlobinfo() {
		return blobinfo;
	}

	public void setBlobinfo(String blobinfo) {
		this.blobinfo = blobinfo;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public Integer getRunId() {
		return runId;
	}

	public void setRunId(Integer runId) {
		this.runId = runId;
	}

	public Integer getJobAttempt() {
		return jobAttempt;
	}

	public void setJobAttempt(Integer jobAttempt) {
		this.jobAttempt = jobAttempt;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Date getEndTime() {
		return endTime;
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public String getCountMatch() {
		return countMatch;
	}

	public void setCountMatch(String countMatch) {
		this.countMatch = countMatch;
	}

	public int getDestRecord() {
		return destRecord;
	}

	public void setDestRecord(int destRecord) {
		this.destRecord = destRecord;
	}

	public String getSourceRecord() {
		return sourceRecord;
	}

	public void setSourceRecord(String sourceRecord) {
		this.sourceRecord = sourceRecord;
	}

	public int getReqId() {
		return reqId;
	}

	public void setReqId(int reqId) {
		this.reqId = reqId;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String queryTitle) {
		this.query = queryTitle;
	}

}
