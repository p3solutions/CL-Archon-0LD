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
@Table(name = "LOTUSNOTES_SESSIONS", indexes = { @Index(name = "ma_index1", columnList = "HOST"),
		@Index(name = "ma_index2", columnList = "PORT"), @Index(name = "ma_index3", columnList = "DB"),
		@Index(name = "ma_index4", columnList = "USER_NAME") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class LotusNotesSessions {

	@Id
	@GeneratedValue(generator = "UUID")
	@GenericGenerator(name = "UUID", strategy = "org.hibernate.id.UUIDGenerator")
	@Column(name = "SESSION_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String sessonId;

	@Column(name = "HOST", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String host;

	@Column(name = "PORT", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String port;

	@Column(name = "DB", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String database;

	@Column(name = "USER_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userName;

	@Column(name = "FORM_DETAILS", nullable = true, columnDefinition = "json DEFAULT NULL")
	String formDetails;

	@Column(name = "PROFILE_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String profileName;

	@Column(name = "SAVE_DATE", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp saveDate;

	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userId;

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getSessonId() {
		return sessonId;
	}

	public void setSessonId(String sessonId) {
		this.sessonId = sessonId;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getProfileName() {
		return profileName;
	}

	public void setProfileName(String profileName) {
		this.profileName = profileName;
	}

	public Timestamp getSaveDate() {
		return saveDate;
	}

	public void setSaveDate(Timestamp saveDate) {
		this.saveDate = saveDate;
	}

	public String getFormDetails() {
		return formDetails;
	}

	public void setFormDetails(String formDetails) {
		this.formDetails = formDetails;
	}
}
