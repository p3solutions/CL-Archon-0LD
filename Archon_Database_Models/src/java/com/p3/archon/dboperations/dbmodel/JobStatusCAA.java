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
import com.p3.archon.dboperations.dbmodel.enums.JobStatus_CAA;


@SuppressWarnings("deprecation")
@Entity
@Table(name = "JOB_STATUS_CAA" , indexes = { @Index(name = "job_status_caa_index", columnList = "JOB_ID"),
		@Index(name = "job_status_caa_index1", columnList = "USER_ID") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class JobStatusCAA {

	@Id
	@Column(name = "JOB_ID")
	String jobId;
	
	@Column(name = "USER_ID")
	String userId;
	
	@Column(name = "UPDATED_DT")
	Timestamp updatedDt;
	
	@Enumerated(EnumType.ORDINAL)
	@Column(name = "S1_STATUS")
	JobStatus_CAA s1Status;
	
	@Enumerated(EnumType.ORDINAL)
	@Column(name = "S2_STATUS")
	JobStatus_CAA s2Status;
	
	@Enumerated(EnumType.ORDINAL)
	@Column(name = "S3_STATUS")
	JobStatus_CAA s3Status;
	
	@Enumerated(EnumType.ORDINAL)
	@Column(name = "S4_STATUS")
	JobStatus_CAA s4Status;
	
	@Enumerated(EnumType.ORDINAL)
	@Column(name = "S5_STATUS")
	JobStatus_CAA s5Status;
	
	@Enumerated(EnumType.ORDINAL)
	@Column(name = "S6_STATUS")
	JobStatus_CAA s6Status;
	
	@Enumerated(EnumType.ORDINAL)
	@Column(name = "S7_STATUS")
	JobStatus_CAA s7Status;
	
	@Enumerated(EnumType.ORDINAL)
	@Column(name = "S8_STATUS")
	JobStatus_CAA s8Status;
	
	@Column(name = "STAGE")
	Double stage;
	
	@Enumerated(EnumType.ORDINAL)
	@Column(name = "WF1_STATUS")
	JobStatus_CAA wf1Status;
	
	@Enumerated(EnumType.ORDINAL)
	@Column(name = "WF2_STATUS")
	JobStatus_CAA wf2Status;
	
	@Enumerated(EnumType.ORDINAL)
	@Column(name = "WF3_STATUS")
	JobStatus_CAA wf3Status;
	
	@Enumerated(EnumType.ORDINAL)
	@Column(name = "WF4_STATUS")
	JobStatus_CAA wf4Status;
	
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

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public Timestamp getUpdatedDt() {
		return updatedDt;
	}

	public void setUpdatedDt(Timestamp updatedDt) {
		this.updatedDt = updatedDt;
	}

	public JobStatus_CAA getS1Status() {
		return s1Status;
	}

	public void setS1Status(JobStatus_CAA s1Status) {
		this.s1Status = s1Status;
	}

	public JobStatus_CAA getS2Status() {
		return s2Status;
	}

	public void setS2Status(JobStatus_CAA s2Status) {
		this.s2Status = s2Status;
	}

	public JobStatus_CAA getS3Status() {
		return s3Status;
	}

	public void setS3Status(JobStatus_CAA s3Status) {
		this.s3Status = s3Status;
	}

	public JobStatus_CAA getS4Status() {
		return s4Status;
	}

	public void setS4Status(JobStatus_CAA s4Status) {
		this.s4Status = s4Status;
	}

	public JobStatus_CAA getS5Status() {
		return s5Status;
	}

	public void setS5Status(JobStatus_CAA s5Status) {
		this.s5Status = s5Status;
	}

	public JobStatus_CAA getS6Status() {
		return s6Status;
	}

	public void setS6Status(JobStatus_CAA s6Status) {
		this.s6Status = s6Status;
	}

	public JobStatus_CAA getS7Status() {
		return s7Status;
	}

	public void setS7Status(JobStatus_CAA s7Status) {
		this.s7Status = s7Status;
	}

	public JobStatus_CAA getS8Status() {
		return s8Status;
	}

	public void setS8Status(JobStatus_CAA s8Status) {
		this.s8Status = s8Status;
	}

	public Double getStage() {
		return stage;
	}

	public void setStage(Double stage) {
		this.stage = stage;
	}

	public JobStatus_CAA getWf1Status() {
		return wf1Status;
	}

	public void setWf1Status(JobStatus_CAA wf1Status) {
		this.wf1Status = wf1Status;
	}

	public JobStatus_CAA getWf2Status() {
		return wf2Status;
	}

	public void setWf2Status(JobStatus_CAA wf2Status) {
		this.wf2Status = wf2Status;
	}

	public JobStatus_CAA getWf3Status() {
		return wf3Status;
	}

	public void setWf3Status(JobStatus_CAA wf3Status) {
		this.wf3Status = wf3Status;
	}

	public JobStatus_CAA getWf4Status() {
		return wf4Status;
	}

	public void setWf4Status(JobStatus_CAA wf4Status) {
		this.wf4Status = wf4Status;
	}
	
}
