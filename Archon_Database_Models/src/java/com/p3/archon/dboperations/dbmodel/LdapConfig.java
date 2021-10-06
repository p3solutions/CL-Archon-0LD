package com.p3.archon.dboperations.dbmodel;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "LDAP_CONFIG")
public class LdapConfig {

	@Id
	@Column(name = "CONF_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String confName;

	@Column(name = "HOST",nullable = true, columnDefinition = "varchar(255) NOT NULL")
	String host;

	@Column(name = "PORT", nullable = true, columnDefinition = "varchar(255) NOT NULL")
	String port;

	@Column(name = "SECURITY_AUTHENTICATION",nullable = true, columnDefinition = "varchar(255) NOT NULL")
	String securityAuth;
	
	@Column(name = "USER_ATTRIBUTE", nullable = true, columnDefinition = "varchar(255) NOT NULL")
	String userAttrib;
	
	@Column(name = "EMAIL_ATTRIBUTE", nullable = true, columnDefinition = "varchar(255) NOT NULL")
	String emailAttrib;
	
	@Column(name = "FIRSTNAME_ATTRIBUTE", nullable = true, columnDefinition = "varchar(255) NOT NULL")
	String firstnameAttrib;
	
	@Column(name = "LASTNAME_ATTRIBUTE", nullable = true, columnDefinition = "varchar(255) NOT NULL")
	String lastnameAttrib;
	
	@Column(name = "DISTINGUISHED_NAME", nullable = true, columnDefinition = "varchar(255) NOT NULL")
	String distinguishedName;

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

	public String getSecurityAuth() {
		return securityAuth;
	}

	public void setSecurityAuth(String securityAuth) {
		this.securityAuth = securityAuth;
	}

	public String getUserAttrib() {
		return userAttrib;
	}

	public void setUserAttrib(String userAttrib) {
		this.userAttrib = userAttrib;
	}

	public String getEmailAttrib() {
		return emailAttrib;
	}

	public void setEmailAttrib(String emailAttrib) {
		this.emailAttrib = emailAttrib;
	}

	public String getFirstnameAttrib() {
		return firstnameAttrib;
	}

	public void setFirstnameAttrib(String firstnameAttrib) {
		this.firstnameAttrib = firstnameAttrib;
	}

	public String getLastnameAttrib() {
		return lastnameAttrib;
	}

	public void setLastnameAttrib(String lastnameAttrib) {
		this.lastnameAttrib = lastnameAttrib;
	}

	public String getDistinguishedName() {
		return distinguishedName;
	}

	public void setDistinguishedName(String distinguishedName) {
		this.distinguishedName = distinguishedName;
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
