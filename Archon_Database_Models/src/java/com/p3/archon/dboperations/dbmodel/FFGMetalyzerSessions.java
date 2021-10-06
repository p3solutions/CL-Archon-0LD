package com.p3.archon.dboperations.dbmodel;

import java.sql.Blob;
import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;

import com.p3.archon.dboperations.dbmodel.enums.JobRunType;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "FFGMETALYZER_SESSIONS", indexes = { @Index(name = "fma_index", columnList = "USER_ID"),
		@Index(name = "fma_index1", columnList = "SERVER"), @Index(name = "fma_index2", columnList = "HOST"),
		@Index(name = "fma_index3", columnList = "PORT"), @Index(name = "fma_index4", columnList = "DB"),
		@Index(name = "fma_index5", columnList = "SCHEMA_NAME") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class FFGMetalyzerSessions {

	@Id
	@GeneratedValue(generator = "UUID")
	@GenericGenerator(name = "UUID", strategy = "org.hibernate.id.UUIDGenerator")
	@Column(name = "SESSION_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String sessonId;

	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userId;

	@Column(name = "SERVER", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String server;

	@Column(name = "HOST", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String host;

	@Column(name = "PORT", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String port;

	@Column(name = "DB", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String database;

	@Column(name = "SCHEMA_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String schema;

	@Column(name = "FILE_TYPE", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String fileType;

	@Column(name = "DELIMITER", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String delimiter;

	@Column(name = "JOB_RUN_TYPE", nullable = false, columnDefinition = "int(11) NOT NULL")
	JobRunType jobRunType;

	@Column(name = "BLOB_1", nullable = false)
	Blob blob1;

	@Column(name = "BLOB_2", nullable = false)
	Blob blob2;

	@Column(name = "BLOB_3", nullable = false)
	Blob blob3;

	@Column(name = "BLOB_4", nullable = false)
	Blob blob4;

	@Column(name = "BLOB_5", nullable = false)
	Blob blob5;

	@Column(name = "BLOB_6", nullable = false)
	Blob blob6;

	@Column(name = "PROFILE_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String profileName;

	@Column(name = "SAVE_DATE", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp saveDate;

	public String getSessonId() {
		return sessonId;
	}

	public void setSessonId(String sessonId) {
		this.sessonId = sessonId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
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

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public Blob getBlob1() {
		return blob1;
	}

	public void setBlob1(Blob blob1) {
		this.blob1 = blob1;
	}

	public Blob getBlob2() {
		return blob2;
	}

	public void setBlob2(Blob blob2) {
		this.blob2 = blob2;
	}

	public Blob getBlob3() {
		return blob3;
	}

	public void setBlob3(Blob blob3) {
		this.blob3 = blob3;
	}

	public Blob getBlob4() {
		return blob4;
	}

	public void setBlob4(Blob blob4) {
		this.blob4 = blob4;
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

	public Blob getBlob5() {
		return blob5;
	}

	public void setBlob5(Blob blob5) {
		this.blob5 = blob5;
	}

	public Blob getBlob6() {
		return blob6;
	}

	public void setBlob6(Blob blob6) {
		this.blob6 = blob6;
	}

	public String getFileType() {
		return fileType;
	}

	public void setFileType(String fileType) {
		this.fileType = fileType;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public JobRunType getJobRunType() {
		return jobRunType;
	}

	public void setJobRunType(JobRunType jobRunType) {
		this.jobRunType = jobRunType;
	}

}
