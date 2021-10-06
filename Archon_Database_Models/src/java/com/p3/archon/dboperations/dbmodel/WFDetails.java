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

import com.p3.archon.dboperations.dbmodel.enums.DBType;
import com.p3.archon.dboperations.dbmodel.enums.ToolName;
import com.p3.archon.dboperations.dbmodel.enums.WfStatus;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "WF_DETAILS", indexes = { @Index(name = "wf_details_index", columnList = "WF_ID"),
		@Index(name = "wf_details_index1", columnList = "WF_STATUS"),
		@Index(name = "wf_details_index2", columnList = "ASSIGNED_GROUP") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class WFDetails {

	@Id
	@Column(name = "WF_ID")
	String wfId;

	@Column(name = "WF_NAME")
	String wfName;

	@Column(name = "TABLE_COUNT")
	int tableCnt;

	@Column(name = "JOB_COUNT")
	int jobCnt;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "DB_TYPE_CATEGORY")
	ToolName dbCategory;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "DB_TYPE_IDENTIFIER")
	DBType dbIdentifier;

	@Column(name = "BLOB_1", nullable = true)
	Blob blob1;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "WF_STATUS")
	WfStatus wfStatus;

	@Column(name = "CREATED_DT")
	Timestamp createdDate;

	@Column(name = "CREATED_BY")
	String createdBy;

	@Column(name = "LAST_ACCESSED_DT")
	Timestamp lastAccessDate;

	@Column(name = "LAST_ACCESSED_BY")
	String lastAccessedBy;

	@Column(name = "ASSIGNED_GROUP")
	String assignedGroup;

	@Column(name = "REPLACEMENT_MAP", nullable = true, columnDefinition = "varchar(14000) DEFAULT NULL")
	String replacementMap;

	public String getWfId() {
		return wfId;
	}

	public void setWfId(String wfId) {
		this.wfId = wfId;
	}

	public String getWfName() {
		return wfName;
	}

	public void setWfName(String wfName) {
		this.wfName = wfName;
	}

	public int getTableCnt() {
		return tableCnt;
	}

	public void setTableCnt(int tableCnt) {
		this.tableCnt = tableCnt;
	}

	public int getJobCnt() {
		return jobCnt;
	}

	public void setJobCnt(int jobCnt) {
		this.jobCnt = jobCnt;
	}

	public ToolName getDbCategory() {
		return dbCategory;
	}

	public void setDbCategory(ToolName dbCategory) {
		this.dbCategory = dbCategory;
	}

	public DBType getDbIdentifier() {
		return dbIdentifier;
	}

	public void setDbIdentifier(DBType dbIdentifier) {
		this.dbIdentifier = dbIdentifier;
	}

	public Timestamp getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Timestamp createdDate) {
		this.createdDate = createdDate;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public Timestamp getLastAccessDate() {
		return lastAccessDate;
	}

	public void setLastAccessDate(Timestamp lastAccessDate) {
		this.lastAccessDate = lastAccessDate;
	}

	public String getLastAccessedBy() {
		return lastAccessedBy;
	}

	public void setLastAccessedBy(String lastAccessedBy) {
		this.lastAccessedBy = lastAccessedBy;
	}

	public String getAssignedGroup() {
		return assignedGroup;
	}

	public void setAssignedGroup(String assignedGroup) {
		this.assignedGroup = assignedGroup;
	}

	public String getReplacementMap() {
		return replacementMap;
	}

	public void setReplacementMap(String replacementMap) {
		this.replacementMap = replacementMap;
	}

	public Blob getBlob1() {
		return blob1;
	}

	public void setBlob1(Blob blob1) {
		this.blob1 = blob1;
	}

	public WfStatus getWfStatus() {
		return wfStatus;
	}

	public void setWfStatus(WfStatus wfStatus) {
		this.wfStatus = wfStatus;
	}

}
