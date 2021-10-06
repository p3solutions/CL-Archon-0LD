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

@SuppressWarnings("deprecation")
@Entity
@Table(name = "AS400_METALYZER_SESSIONS", indexes = { @Index(name = "as400_ma_index", columnList = "USER_ID"),
		@Index(name = "as400_ma_index1", columnList = "HOST"), @Index(name = "as400_ma_index2", columnList = "PORT"),
		@Index(name = "as400_ma_index3", columnList = "LIB"),
		@Index(name = "as400_ma_index4", columnList = "USER_NAME"),
		@Index(name = "as400_ma_index5", columnList = "BASE_IND") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class As400MetalyzerSessions {

	@Id
	@GeneratedValue(generator = "UUID")
	@GenericGenerator(name = "UUID", strategy = "org.hibernate.id.UUIDGenerator")
	@Column(name = "SESSION_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String sessonId;

	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userId;

	@Column(name = "HOST", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String host;

	@Column(name = "PORT", nullable = true, columnDefinition = "varchar(255)")
	String port;

	@Column(name = "LIB", nullable = true, columnDefinition = "varchar(255)")
	String lib;

	@Column(name = "USER_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userName;

	@Column(name = "BASE_IND", nullable = false, columnDefinition = "char(1) NOT NULL")
	String baseInd;

	/*
	 * 
	 * ms.setBlob1(blobtableListBean); ms.setBlob2(blobjoinMap);
	 * ms.setBlob3(blobtypeMap); ms.setBlob4(blobtableJoinInfoMap);
	 * ms.setBlob5(blobspvType); ms.setBlob6(blobspvJoins);
	 * 
	 */
	@Column(name = "BLOB_SCHEMALIST", nullable = true)
	Blob blobSchemaList;

	@Column(name = "BLOB_1", nullable = true)
	Blob blob1;

	@Column(name = "BLOB_2", nullable = true)
	Blob blob2;

	@Column(name = "BLOB_3", nullable = true)
	Blob blob3;

	// @Column(name = "BLOB_5", nullable = true) to be used later for SPV Analysis
	// Blob blob5;
	//
	// @Column(name = "BLOB_6", nullable = true)
	// Blob blob6;

	@Column(name = "BLOB_SELECTIONLIST", nullable = true) // for schema table selection
	Blob blobSelectionList;

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

	public String getLib() {
		return lib;
	}

	public void setLib(String lib) {
		this.lib = lib;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
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

	public String getBaseInd() {
		return baseInd;
	}

	public void setBaseInd(String baseInd) {
		this.baseInd = baseInd;
	}

	public Blob getBlobSchemaList() {
		return blobSchemaList;
	}

	public void setBlobSchemaList(Blob blobSchemaList) {
		this.blobSchemaList = blobSchemaList;
	}

	public Blob getBlobSelectionList() {
		return blobSelectionList;
	}

	public void setBlobSelectionList(Blob blobSelectionList) {
		this.blobSelectionList = blobSelectionList;
	}

}
