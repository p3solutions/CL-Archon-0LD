package com.p3.archon.dboperations.dbmodel;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import com.p3.archon.constants.BuildConstants;
import com.p3.archon.dboperations.dbmodel.enums.ServerType;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "SERVER_INFO_LIST", indexes = { @Index(name = "server_info_list_index", columnList = "SERVER_HANDLER") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class ServerInfoList {

	@Id
	@Column(name = "SERVER_HANDLER", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String serverHandler;

	@Column(name = "SERVER_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String serverName;

	@Column(name = "OS", nullable = false, columnDefinition = "varchar(10) NOT NULL")
	String osTypeId;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "SERVER_REGION", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	ServerType serverRegion;

	@Column(name = "SERVER_DESC", nullable = false, columnDefinition = "varchar(255) NOT NULL DEFAULT ''")
	String serverDesc;

	@Column(name = "DD_ID", nullable = false, columnDefinition = "varchar(500) NOT NULL")
	String ddId;

	@Column(name = "REGISTERED_DATE", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp registeredDate;

	@Column(name = "UPDATED_BY", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String updatedBy;

	@Column(name = "VERSION", nullable = false, columnDefinition = "varchar(255) NOT NULL DEFAULT '"
			+ BuildConstants.BUILD + "'")
	String version;

	@Column(name = "IS_ACTIVE", nullable = false, columnDefinition = "TINYINT(1) DEFAULT 1")
	boolean active;

	@Column(name = "LAST_UPDATED_TIME", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp updatedTime;
	
	@Column(name = "CONFIG_MAC_ADDRESS", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String configSysMacAddr;
	
	@Column(name = "DB_SOURCE_INFO", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String dbSourceInfo;

	public Timestamp getUpdatedTime() {
		return updatedTime;
	}

	public void setUpdatedTime(Timestamp updatedTime) {
		this.updatedTime = updatedTime;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getOsTypeId() {
		return osTypeId;
	}

	public void setOsTypeId(String osTypeId) {
		this.osTypeId = osTypeId;
	}

	public String getDdId() {
		return ddId;
	}

	public void setDdId(String ddId) {
		this.ddId = ddId;
	}

	public String getServerHandler() {
		return serverHandler;
	}

	public void setServerHandler(String serverHandler) {
		this.serverHandler = serverHandler;
	}

	public String getServerName() {
		return serverName;
	}

	public void setServerName(String serverName) {
		this.serverName = serverName;
	}

	public ServerType getServerRegion() {
		return serverRegion;
	}

	public void setServerRegion(ServerType serverRegion) {
		this.serverRegion = serverRegion;
	}

	public String getServerDesc() {
		return serverDesc;
	}

	public void setServerDesc(String serverDesc) {
		this.serverDesc = serverDesc;
	}

	public Timestamp getRegisteredDate() {
		return registeredDate;
	}

	public void setRegisteredDate(Timestamp registeredDate) {
		this.registeredDate = registeredDate;
	}

	public String getUpdatedBy() {
		return updatedBy;
	}

	public void setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
	}

	public String getConfigSysMacAddr() {
		return configSysMacAddr;
	}

	public void setConfigSysMacAddr(String configSysMacAddr) {
		this.configSysMacAddr = configSysMacAddr;
	}

	public String getDbSourceInfo() {
		return dbSourceInfo;
	}

	public void setDbSourceInfo(String dbSourceInfo) {
		this.dbSourceInfo = dbSourceInfo;
	}

}