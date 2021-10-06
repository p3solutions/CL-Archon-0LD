package com.p3.archon.dboperations.dbmodel;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Index;
import javax.persistence.Table;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "CRED_MANAGER", indexes = { @Index(name = "cred_index", columnList = "WF_ID"),
		@Index(name = "cred_index1", columnList = "USER_ID") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class CredentialManager {

	@EmbeddedId
	ConnectionId id;

	@Column(name = "CONN_USERNAME")
	String dbuser;

	@Column(name = "CONN_PASSWORD")
	String password;

	@Column(name = "CONN_SECRET_KEY")
	String secKey;

	@Column(name = "CREATED_DT")
	Timestamp createdDate;

	@Column(name = "LAST_ACCESSED_DT")
	Timestamp lastAccessDate;

	public ConnectionId getId() {
		return id;
	}

	public void setId(ConnectionId id) {
		this.id = id;
	}

	public String getDbuser() {
		return dbuser;
	}

	public void setDbuser(String dbuser) {
		this.dbuser = dbuser;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getSecKey() {
		return secKey;
	}

	public void setSecKey(String secKey) {
		this.secKey = secKey;
	}

	public Timestamp getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Timestamp createdDate) {
		this.createdDate = createdDate;
	}

	public Timestamp getLastAccessDate() {
		return lastAccessDate;
	}

	public void setLastAccessDate(Timestamp lastAccessDate) {
		this.lastAccessDate = lastAccessDate;
	}

}