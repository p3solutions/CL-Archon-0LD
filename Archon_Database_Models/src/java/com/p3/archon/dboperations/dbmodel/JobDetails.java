package com.p3.archon.dboperations.dbmodel;

import java.io.Serializable;
import java.sql.Blob;
import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Index;
import javax.persistence.Table;

import com.p3.archon.dboperations.dbmodel.enums.JobInterval;
import com.p3.archon.dboperations.dbmodel.enums.JobRunType;
import com.p3.archon.dboperations.dbmodel.enums.JobStatus;
import com.p3.archon.dboperations.dbmodel.enums.ToolCategory;
import com.p3.archon.dboperations.dbmodel.enums.ToolName;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "JOB_DETAILS", indexes = { @Index(name = "job_details_index", columnList = "JOB_ID"),
		@Index(name = "job_details_index1", columnList = "RUN_ID"),
		@Index(name = "job_details_index2", columnList = "JOB_ATTEMPT"),
		@Index(name = "job_details_index3", columnList = "TOOL_NAME"),
		@Index(name = "job_details_index4", columnList = "TOOL_CATEGORY"),
		@Index(name = "job_details_index5", columnList = "JOB_RUN_TYPE"),
		@Index(name = "job_details_index6", columnList = "JOB_INTERVAL"),
		@Index(name = "job_details_index7", columnList = "SCHEDULED_TIME"),
		@Index(name = "job_details_index8", columnList = "USER_ID"),
		@Index(name = "job_details_index9", columnList = "STATUS"),
		@Index(name = "job_details_index10", columnList = "JOB_RETRIED") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class JobDetails implements Serializable {

	private static final long serialVersionUID = 1L;

	@EmbeddedId
	RowId rowId;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "TOOL_NAME", nullable = false, columnDefinition = "int(11) NOT NULL")
	ToolName toolName;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "TOOL_CATEGORY", nullable = false, columnDefinition = "int(11) NOT NULL")
	ToolCategory toolCategory;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "JOB_RUN_TYPE", nullable = false, columnDefinition = "int(11) DEFAULT 0")
	JobRunType jobRunType;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "JOB_INTERVAL", nullable = true, columnDefinition = "int(11) DEFAULT NULL")
	JobInterval jobInterval;

	@Column(name = "JOB_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String jobName;

	@Column(name = "JOB_FREQUENCY", nullable = true, columnDefinition = "int(10) DEFAULT NULL")
	Integer jobFrequency;

	@Column(name = "INPUT_DETAILS", nullable = true, columnDefinition = "json DEFAULT NULL")
	String inputDetails;

	@Column(name = "SCHEDULED_TIME", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp scheduledTime;

	@Column(name = "START_TIME", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp startTime;

	@Column(name = "END_TIME", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp endTime;

	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userId;

	@Column(name = "USER_EMAIL", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userEmail;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "STATUS", nullable = false, columnDefinition = "int(11) DEFAULT 0")
	JobStatus status;

	@Column(name = "JOB_RETRIED", nullable = false, columnDefinition = "Boolean DEFAULT 0")
	Boolean jobRetried;

	@Column(name = "MESSAGE", nullable = true, columnDefinition = "varchar(14000) DEFAULT NULL")
	String message;

	@Column(name = "HANDLER_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String handler;

	@Column(name = "AUDIT_ZIP_FILE", nullable = true)
	Blob auditLogZipFile;

	@Column(name = "META_BLOB", nullable = true)
	Blob metaBlob;

	public Blob getMetaBlob() {
		return metaBlob;
	}

	public void setMetaBlob(Blob metaBlob) {
		this.metaBlob = metaBlob;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public Blob getAuditLogZipFile() {
		return auditLogZipFile;
	}

	public void setAuditLogZipFile(Blob auditLogZipFile) {
		this.auditLogZipFile = auditLogZipFile;
	}

	public String getHandler() {
		return handler;
	}

	public void setHandler(String handler) {
		this.handler = handler;
	}

	public RowId getRowId() {
		return rowId;
	}

	public String getRowIdString() {
		return rowId.getJobId() + "~" + rowId.getJobAttempt() + "~" + rowId.getRunId();
	}

	public void setRowId(RowId rowId) {
		this.rowId = rowId;
	}

	public ToolName getToolName() {
		return toolName;
	}

	public void setToolName(ToolName toolName) {
		this.toolName = toolName;
	}

	public ToolCategory getToolCategory() {
		return toolCategory;
	}

	public void setToolCategory(ToolCategory toolCategory) {
		this.toolCategory = toolCategory;
	}

	public JobRunType getJobRunType() {
		return jobRunType;
	}

	public void setJobRunType(JobRunType jobRunType) {
		this.jobRunType = jobRunType;
	}

	public JobInterval getJobInterval() {
		return jobInterval;
	}

	public void setJobInterval(JobInterval jobInterval) {
		this.jobInterval = jobInterval;
	}

	public Integer getJobFrequency() {
		return jobFrequency;
	}

	public void setJobFrequency(Integer jobFrequency) {
		this.jobFrequency = jobFrequency;
	}

	public String getInputDetails() {
		return inputDetails;
	}

	public void setInputDetails(String inputDetails) {
		this.inputDetails = inputDetails;
	}

	public Timestamp getScheduledTime() {
		return scheduledTime;
	}

	public void setScheduledTime(Timestamp scheduledTime) {
		this.scheduledTime = scheduledTime;
	}

	public Timestamp getStartTime() {
		return startTime;
	}

	public void setStartTime(Timestamp startTime) {
		this.startTime = startTime;
	}

	public Timestamp getEndTime() {
		return endTime;
	}

	public void setEndTime(Timestamp endTime) {
		this.endTime = endTime;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getUserEmail() {
		return userEmail;
	}

	public void setUserEmail(String userEmail) {
		this.userEmail = userEmail;
	}

	public JobStatus getStatus() {
		return status;
	}

	public void setStatus(JobStatus status) {
		this.status = status;
	}

	public Boolean getJobRetried() {
		return jobRetried;
	}

	public void setJobRetried(Boolean jobRetried) {
		this.jobRetried = jobRetried;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

}