package com.p3.archon.dboperations.dbmodel;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import com.p3.archon.dboperations.dbmodel.enums.ErtJobStatus;
import com.p3.archon.dboperations.dbmodel.enums.ErtJobType;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "ERT_JOB_DETAILS", indexes = { @Index(name = "ert_job_details_index", columnList = "WF_ID"),
		@Index(name = "ert_job_details_index1", columnList = "JOB_ID"),
		@Index(name = "ert_job_details_index2", columnList = "JOB_STATUS") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class ErtJobDetails {

	@Column(name = "WF_ID")
	String wfId;
	
	@Id
	@Column(name = "JOB_ID")
	String jobId;
	
	@Column(name = "JOB_NAME")
	String jobName;
	
	@Enumerated(EnumType.ORDINAL)
	@Column(name = "JOB_TYPE")
	ErtJobType jobType;

	@Column(name = "CREATED_DT")
	Timestamp createdDt;
	
	@Column(name = "CREATED_BY")
	String createdBy;
	
	@Column(name = "LAST_ACCESSED_DT")
	Timestamp lastAccessDt;
	
	@Column(name = "LAST_ACCESSED_BY")
	String lastAccessedBy;
	
	@Enumerated(EnumType.ORDINAL)
	@Column(name = "JOB_STATUS")
	ErtJobStatus jobStatus;
	
	@Column(name = "SAVED_JOB_DETAILS", columnDefinition = "json DEFAULT NULL")
	String savedJobDetails;

	public String getWfId() {
		return wfId;
	}

	public void setWfId(String wfId) {
		this.wfId = wfId;
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

	public ErtJobType getJobType() {
		return jobType;
	}

	public void setJobType(ErtJobType jobType) {
		this.jobType = jobType;
	}

	public Timestamp getCreatedDt() {
		return createdDt;
	}

	public void setCreatedDt(Timestamp createdDt) {
		this.createdDt = createdDt;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public Timestamp getLastAccessDt() {
		return lastAccessDt;
	}

	public void setLastAccessDt(Timestamp lastAccessDt) {
		this.lastAccessDt = lastAccessDt;
	}

	public String getLastAccessedBy() {
		return lastAccessedBy;
	}

	public void setLastAccessedBy(String lastAccessedBy) {
		this.lastAccessedBy = lastAccessedBy;
	}

	public ErtJobStatus getJobStatus() {
		return jobStatus;
	}

	public void setJobStatus(ErtJobStatus jobStatus) {
		this.jobStatus = jobStatus;
	}

	public String getSavedJobDetails() {
		return savedJobDetails;
	}

	public void setSavedJobDetails(String savedJobDetails) {
		this.savedJobDetails = savedJobDetails;
	}



}
