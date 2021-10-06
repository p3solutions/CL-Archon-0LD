package com.p3.archon.dboperations.dbmodel;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "PURGE_USER_STATISTICS", indexes = { @Index(name = "ph_index", columnList = "PURGE_TIME"),
		@Index(name = "ph_index1", columnList = "USER_ID"), @Index(name = "ph_index2", columnList = "ID") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class PurgeUserStatistics {

	@Id
	@GenericGenerator(name = "gen", strategy = "increment")
	@GeneratedValue(generator = "gen")
	@Column(name = "ID", unique = true, nullable = false, precision = 15, scale = 0)
	int id;

	@Column(name = "USER_ID", nullable = false)
	String userId;

	@Column(name = "PURGE_TIME", nullable = false, columnDefinition = "datetime(6)")
	Timestamp purgeTime;

	@Column(name = "JOB_DETAILS_COMPLETED_COUNT", nullable = false, columnDefinition = "int(10) DEFAULT 0")
	int jdCompletedValue;

	@Column(name = "JOB_DETAILS_FAILED_COUNT", nullable = false, columnDefinition = "int(10) DEFAULT 0")
	int jdFailedValue;

	@Column(name = "JOB_DETAILS_CANCELLED_COUNT", nullable = false, columnDefinition = "int(10) DEFAULT 0")
	int jdCancelledValue;

	@Column(name = "SCHEDULE_ENTRY_COMPLETED_COUNT", nullable = false, columnDefinition = "int(10) DEFAULT 0")
	int seCompletedValue;

	@Column(name = "SCHEDULE_ENTRY_FAILED_COUNT", nullable = false, columnDefinition = "int(10) DEFAULT 0")
	int seFailedValue;

	@Column(name = "SCHEDULE_ENTRY_CANCELLED_COUNT", nullable = false, columnDefinition = "int(10) DEFAULT 0")
	int seCancelledValue;

	public PurgeUserStatistics(String userId, Timestamp purgeDate) {
		this.userId = userId;
		this.purgeTime = purgeDate;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public Timestamp getPurgeTime() {
		return purgeTime;
	}

	public void setPurgeTime(Timestamp purgeTime) {
		this.purgeTime = purgeTime;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public int getJdCompletedValue() {
		return jdCompletedValue;
	}

	public void setJdCompletedValue(int jdCompletedValue) {
		this.jdCompletedValue = jdCompletedValue;
	}

	public int getJdFailedValue() {
		return jdFailedValue;
	}

	public void setJdFailedValue(int jdFailedValue) {
		this.jdFailedValue = jdFailedValue;
	}

	public int getJdCancelledValue() {
		return jdCancelledValue;
	}

	public void setJdCancelledValue(int jdCancelledValue) {
		this.jdCancelledValue = jdCancelledValue;
	}

	public int getSeCompletedValue() {
		return seCompletedValue;
	}

	public void setSeCompletedValue(int seCompletedValue) {
		this.seCompletedValue = seCompletedValue;
	}

	public int getSeFailedValue() {
		return seFailedValue;
	}

	public void setSeFailedValue(int seFailedValue) {
		this.seFailedValue = seFailedValue;
	}

	public int getSeCancelledValue() {
		return seCancelledValue;
	}

	public void setSeCancelledValue(int seCancelledValue) {
		this.seCancelledValue = seCancelledValue;
	}

}
