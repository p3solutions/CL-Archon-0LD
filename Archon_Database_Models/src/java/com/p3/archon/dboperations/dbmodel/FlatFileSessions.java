package com.p3.archon.dboperations.dbmodel;

import java.sql.Blob;
import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import com.p3.archon.dboperations.dbmodel.enums.FlatFileJobStatus;
import com.p3.archon.dboperations.dbmodel.enums.FlatFileJobType;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "FLAT_FILE_SESSIONS", indexes = { @Index(name = "flat_file_job_details_index", columnList = "JOB_ID"),
		@Index(name = "flat_file_details_index1", columnList = "JOB_NAME"),
		@Index(name = "flat_file_details_index2", columnList = "JOB_STATUS") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class FlatFileSessions {

	@Id
	@Column(name = "JOB_ID")
	String jobId;

	@Column(name = "JOB_NAME")
	String jobName;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "JOB_TYPE")
	FlatFileJobType jobType;

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
	FlatFileJobStatus jobStatus;

	@Column(name = "SAVED_JOB_DETAILS", columnDefinition = "json DEFAULT NULL")
	String savedJobDetails;

	@Column(name = "META_BLOB", nullable = true)
	Blob metadataPathBlob;

	@Column(name = "INPUT_DATA_BLOB", nullable = true)
	Blob inputPathBlob;

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

	public FlatFileJobType getJobType() {
		return jobType;
	}

	public void setJobType(FlatFileJobType jobType) {
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

	public FlatFileJobStatus getJobStatus() {
		return jobStatus;
	}

	public void setJobStatus(FlatFileJobStatus jobStatus) {
		this.jobStatus = jobStatus;
	}

	public String getSavedJobDetails() {
		return savedJobDetails;
	}

	public void setSavedJobDetails(String savedJobDetails) {
		this.savedJobDetails = savedJobDetails;
	}

	public Blob getMetadataPathBlob() {
		return metadataPathBlob;
	}

	public void setMetadataPathBlob(Blob metadataPathBlob) {
		this.metadataPathBlob = metadataPathBlob;
	}

	public Blob getInputPathBlob() {
		return inputPathBlob;
	}

	public void setInputPathBlob(Blob inputPathBlob) {
		this.inputPathBlob = inputPathBlob;
	}

}
