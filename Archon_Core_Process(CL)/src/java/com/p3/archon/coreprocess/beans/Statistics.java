package com.p3.archon.coreprocess.beans;

import java.util.Date;
import java.util.TreeMap;

public class Statistics {

	private Date startTime;
	private Date endTime;
	private int recordsProcessed;
	private String sourceRecords;
	private String status;
	private String error;
	private TreeMap<String, Long> srcBlobCount;
	private TreeMap<String, Long> destBlobCount;

	public Statistics() {
		this.srcBlobCount = new TreeMap<String, Long>();
		this.destBlobCount = new TreeMap<String, Long>(); 
	}
	
	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public Date getEndTime() {
		return endTime;
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}

	public int getRecordsProcessed() {
		return recordsProcessed;
	}

	public void setRecordsProcessed(int recordsProcessed) {
		this.recordsProcessed = recordsProcessed;
	}

	public String getSourceRecords() {
		return sourceRecords;
	}

	public void setSourceRecords(String sourceRecords) {
		this.sourceRecords = sourceRecords;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public TreeMap<String, Long> getDestBlobCount() {
		return destBlobCount;
	}

	public void setDestBlobCount(TreeMap<String, Long> destBlobCount) {
		this.destBlobCount = destBlobCount;
	}

	public TreeMap<String, Long> getSrcBlobCount() {
		return srcBlobCount;
	}

	public void setSrcBlobCount(TreeMap<String, Long> srcBlobCount) {
		this.srcBlobCount = srcBlobCount;
	}
}
