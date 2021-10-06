package com.p3.archon.dboperations.dbmodel;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@Embeddable
public class RowId implements Serializable{

	private static final long serialVersionUID = 1L;
	
	@Column(name="JOB_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String jobId;
	
	@Column(name="RUN_ID", nullable = false, columnDefinition = "int(10) DEFAULT 1")
	private Integer runId;
	
	@Column(name="JOB_ATTEMPT", nullable = false, columnDefinition = "int(10) DEFAULT 0")
	private Integer jobAttempt;
	
	public static long getSerialversionuid() {
		return serialVersionUID;
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

}
