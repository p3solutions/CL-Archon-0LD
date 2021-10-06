package com.p3.archon.dboperations.dbmodel;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "WF_CONNECTIONS", indexes = { @Index(name = "wf_conn_index", columnList = "WF_ID") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class WFConnections {

	@Id
	@Column(name = "WF_ID")
	String wfId;

	@Column(name = "SERVER")
	String server;

	@Column(name = "HOST")
	String host;

	@Column(name = "PORT")
	String port;

	@Column(name = "DB_NAME")
	String database;

	@Column(name = "SCHEMA_NAME")
	String schema;

	@Column(name = "URL")
	String url;

	@Column(name = "CREATED_DT")
	Timestamp createdDate;

	@Column(name = "CREATED_BY")
	String createdBy;

	@Column(name = "LAST_ACCESSED_DT")
	Timestamp lastAccessDate;

	public String getWfId() {
		return wfId;
	}

	public void setWfId(String wfId) {
		this.wfId = wfId;
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

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
}