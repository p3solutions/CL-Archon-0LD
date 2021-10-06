package com.p3.archon.dboperations.dbmodel;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "AUTHENTICATION_CONFIG")
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class AuthenticationConfig {

	@Id
	@Column(name = "CONF_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String confName;

	@Column(name = "CONF_PRI", nullable = true, columnDefinition = "int(11) DEFAULT NULL")
	int confPri;

	@Column(name = "USER_NAME", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String userName;

	@Column(name = "PASSWORD", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String password;

	@Column(name = "SERVER",nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String server;

	@Column(name = "PORT", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String port;

	@Column(name = "DB_NAME",nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String database;

	@Column(name = "UPDATED_BY",nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String updatedBy;

	@Column(name = "UPDATE_DATE",nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp update_date;

	public String getConfName() {
		return confName;
	}

	public void setConfName(String confName) {
		this.confName = confName;
	}

	public int getConfPri() {
		return confPri;
	}

	public void setConfPri(int confPri) {
		this.confPri = confPri;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
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

	public String getUpdatedBy() {
		return updatedBy;
	}

	public void setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
	}

	public Timestamp getUpdate_date() {
		return update_date;
	}

	public void setUpdate_date(Timestamp update_date) {
		this.update_date = update_date;
	}
}
