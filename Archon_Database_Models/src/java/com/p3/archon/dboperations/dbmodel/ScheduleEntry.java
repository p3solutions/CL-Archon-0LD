package com.p3.archon.dboperations.dbmodel;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import com.p3.archon.dboperations.dbmodel.enums.JobInterval;
import com.p3.archon.dboperations.dbmodel.enums.JobRunType;
import com.p3.archon.dboperations.dbmodel.enums.ScheduleStatus;
import com.p3.archon.dboperations.dbmodel.enums.ToolCategory;
import com.p3.archon.dboperations.dbmodel.enums.ToolName;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "SCHEDULE_ENTRY", indexes = { @Index(name = "schedule_entry_index", columnList = "JOB_ID"),
		@Index(name = "schedule_entry_index1", columnList = "TOOL_NAME"),
		@Index(name = "schedule_entry_index2", columnList = "TOOL_CATEGORY"),
		@Index(name = "schedule_entry_index3", columnList = "USER_ID"),
		@Index(name = "schedule_entry_index4", columnList = "STATUS") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class ScheduleEntry {

	@Id
	@Column(name = "JOB_ID", nullable = false, columnDefinition = "varchar(50) NOT NULL")
	String jobId;

	@Column(name = "RUN_ID", nullable = true, columnDefinition = "int(10) DEFAULT NULL")
	Integer runId;

	@Column(name = "FAILURE_COUNT", nullable = false, columnDefinition = "int(10) DEFAULT 0")
	Integer failureCount;

	@Column(name = "JOB_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String jobName;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "TOOL_NAME", nullable = true, columnDefinition = "int(11) DEFAULT NULL")
	ToolName toolName;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "TOOL_CATEGORY", nullable = true, columnDefinition = "int(11) DEFAULT NULL")
	ToolCategory toolCategory;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "JOB_INTERVAL", nullable = true, columnDefinition = "int(11) DEFAULT NULL")
	JobInterval jobInterval;

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

	@Column(name = "LAST_TRIGGERED_TIME", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp lastTriggeredTime;

	@Column(name = "NEXT_TRIGGER_TIME", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp nextTriggerTime;

	@Column(name = "TERMINATED_TIME", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp terminatedTime;

	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userId;

	@Column(name = "USER_EMAIL", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userEmail;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "STATUS", nullable = false, columnDefinition = "int(11) DEFAULT 0")
	ScheduleStatus status;

	@Column(name = "ERROR_MESSAGE", nullable = true, columnDefinition = "varchar(14000) DEFAULT NULL")
	String errorMessage;

	@Column(name = "HANDLER_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String handler;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "JOB_RUN_TYPE", nullable = false, columnDefinition = "int(11) DEFAULT 0")
	JobRunType jobRunType;

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public JobRunType getJobRunType() {
		return jobRunType;
	}

	public void setJobRunType(JobRunType jobRunType) {
		this.jobRunType = jobRunType;
	}

	public String getHandler() {
		return handler;
	}

	public void setHandler(String handler) {
		this.handler = handler;
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

	public Timestamp getLastTriggeredTime() {
		return lastTriggeredTime;
	}

	public void setLastTriggeredTime(Timestamp lastTriggeredTime) {
		this.lastTriggeredTime = lastTriggeredTime;
	}

	public Timestamp getNextTriggerTime() {
		return nextTriggerTime;
	}

	public void setNextTriggerTime(Timestamp nextTriggerTime) {
		this.nextTriggerTime = nextTriggerTime;
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

	public ScheduleStatus getStatus() {
		return status;
	}

	public void setStatus(ScheduleStatus status) {
		this.status = status;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public Timestamp getTerminatedTime() {
		return terminatedTime;
	}

	public void setTerminatedTime(Timestamp terminatedTime) {
		this.terminatedTime = terminatedTime;
	}

	public Integer getFailureCount() {
		return failureCount;
	}

	public void setFailureCount(Integer failureCount) {
		this.failureCount = failureCount;
	}
}