package com.p3.archon.dboperations.dbmodel;

import java.sql.Blob;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "META_PROFILE", indexes = { @Index(name = "meta_profile_index", columnList = "META_ID"),
		@Index(name = "meta_profile_index1", columnList = "USER_ID"),
		@Index(name = "meta_profile_index2", columnList = "SERVER"),
		@Index(name = "meta_profile_index3", columnList = "HOST"),
		@Index(name = "meta_profile_index4", columnList = "PORT"),
		@Index(name = "meta_profile_index5", columnList = "DB"),
		@Index(name = "meta_profile_index6", columnList = "DEFAULT_SCHEMA") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class MetaProfile {

	@Id
	@GeneratedValue(generator = "UUID")
	@GenericGenerator(name = "UUID", strategy = "org.hibernate.id.UUIDGenerator")
	@Column(name = "META_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String metaId;

	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userId;

	@Column(name = "PROFILE_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String profileName;

	@Column(name = "SERVER", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String server;

	@Column(name = "HOST", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String host;

	@Column(name = "PORT", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String port;

	@Column(name = "DB", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String database;

	@Column(name = "DEFAULT_SCHEMA", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String defaultSchema;

	@Column(name = "FULL_OR_SELECTED", nullable = false, columnDefinition = "varchar(255) DEFAULT 'FULL'")
	String fullorSelected;

	@Column(name = "SELECTED_TABLE", nullable = false, columnDefinition = "varchar(255) DEFAULT ''")
	String selectedTable;

	@Column(name = "XML", nullable = false)
	Blob xmlFile;

	public String getMetaId() {
		return metaId;
	}

	public void setMetaId(String metaId) {
		this.metaId = metaId;
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

	public String getDefaultSchema() {
		return defaultSchema;
	}

	public void setDefaultSchema(String defaultSchema) {
		this.defaultSchema = defaultSchema;
	}

	public String getProfileName() {
		return profileName;
	}

	public void setProfileName(String profileName) {
		this.profileName = profileName;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getFullorSelected() {
		return fullorSelected;
	}

	public void setFullorSelected(String fullorSelected) {
		this.fullorSelected = fullorSelected;
	}

	public String getSelectedTable() {
		return selectedTable;
	}

	public void setSelectedTable(String selectedTable) {
		this.selectedTable = selectedTable;
	}

	public Blob getXmlFile() {
		return xmlFile;
	}

	public void setXmlFile(Blob xmlFile) {
		this.xmlFile = xmlFile;
	}

}
