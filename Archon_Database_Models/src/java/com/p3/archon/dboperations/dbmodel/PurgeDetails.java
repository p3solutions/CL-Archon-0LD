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
@Table(name = "PURGE_HISTORY", indexes = { @Index(name = "ph_index", columnList = "PURGE_TIME"),
		@Index(name = "ph_index1", columnList = "ID") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class PurgeDetails {

	@Id
	@GenericGenerator(name = "gen", strategy = "increment")
	@GeneratedValue(generator = "gen")
	@Column(name = "ID", unique = true, nullable = false, precision = 15, scale = 0)
	int id;

	@Column(name = "JOB_DETAILS_PURGE_COUNT", nullable = false, columnDefinition="int DEFAULT 0")
	int jdValue;

	@Column(name = "SCHEDULE_ENTRY_PURGE_COUNT", nullable = false, columnDefinition="int DEFAULT 0")
	int seValue;

	@Column(name = "PURGE_TIME", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp purgeTime;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getJdValue() {
		return jdValue;
	}

	public void setJdValue(int jdValue) {
		this.jdValue = jdValue;
	}

	public int getSeValue() {
		return seValue;
	}

	public void setSeValue(int seValue) {
		this.seValue = seValue;
	}

	public Timestamp getPurgeTime() {
		return purgeTime;
	}

	public void setPurgeTime(Timestamp purgeTime) {
		this.purgeTime = purgeTime;
	}

}
