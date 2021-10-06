package com.p3.archon.dboperations.dbmodel;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "DISASTER_RECOVERY_INFO", indexes = { @Index(name = "disaster_recovery_info_index", columnList = "DISASTER_SERVER_NAME") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class DisasterRecoveryInfo {

	@Id
	@Column(name = "DISASTER_SERVER_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String disasterServer;

	@Column(name = "JOB_SERVER_LIST", nullable = false, columnDefinition = "varchar(12000) NOT NULL")
	String jobServerList;

	@Column(name = "UPDATED_BY", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String updatedBy;

	@Column(name = "LAST_UPDATED_TIME", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp updatedTime;
	

	public String getDisasterServer() {
		return disasterServer;
	}

	public void setDisasterServer(String disasterServer) {
		this.disasterServer = disasterServer;
	}

	public String getJobServerList() {
		return jobServerList;
	}

	public void setJobServerList(String jobServerList) {
		this.jobServerList = jobServerList;
	}

	public String getUpdatedBy() {
		return updatedBy;
	}

	public void setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
	}

	public Timestamp getUpdatedTime() {
		return updatedTime;
	}

	public void setUpdatedTime(Timestamp updatedTime) {
		this.updatedTime = updatedTime;
	}

}