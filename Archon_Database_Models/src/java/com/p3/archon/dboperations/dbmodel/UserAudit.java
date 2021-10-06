package com.p3.archon.dboperations.dbmodel;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import com.p3.archon.dboperations.dbmodel.enums.UserAduitAction;
import com.p3.archon.dboperations.dbmodel.enums.UserAuthenticationType;

@Entity
@Table(name = "USER_AUDIT_DETAILS", indexes = { @Index(name = "user_audit_details_index", columnList = "SNO"),
		@Index(name = "user_audit_details_index1", columnList = "USER_ID"),
		@Index(name = "user_audit_details_index2", columnList = "EMAIL"),
		@Index(name = "user_audit_details_index3", columnList = "ROLE") })
public class UserAudit {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "SNO", updatable = false, nullable = false)
	private Long sNo;

	@Column(name = "IP", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String ip;

	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userId;

	@Column(name = "USER_NAME", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String userName;

	@Column(name = "EMAIL", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String email;

	@Column(name = "ROLE", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String role;

	@Column(name = "ACCESS_TIME", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp accessTime;

	@Column(name = "ACCESS_AUDIT_LOG_ID", nullable = true, columnDefinition = "varchar(255) NOT NULL")
	String accessAuditLogId;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "ACTION", nullable = true, columnDefinition = "int(11) DEFAULT NULL")
	UserAduitAction action;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "AUTHENTICATION_TYPE", nullable = true, columnDefinition = "int(11) DEFAULT NULL")
	UserAuthenticationType authenticationType;

	@Column(name = "COMMENT", nullable = false, columnDefinition = "varchar(14000) DEFAULT ''")
	String comments;

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}

	public Long getsNo() {
		return sNo;
	}

	public void setsNo(Long sNo) {
		this.sNo = sNo;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public Timestamp getAccessTime() {
		return accessTime;
	}

	public void setAccessTime(Timestamp accessTime) {
		this.accessTime = accessTime;
	}

	public UserAduitAction getAction() {
		return action;
	}

	public void setAction(UserAduitAction action) {
		this.action = action;
	}

	public UserAuthenticationType getAuthenticationType() {
		return authenticationType;
	}

	public void setAuthenticationType(UserAuthenticationType authenticationType) {
		this.authenticationType = authenticationType;
	}

	public String getAccessAuditLogId() {
		return accessAuditLogId;
	}

	public void setAccessAuditLogId(String accessAuditLogId) {
		this.accessAuditLogId = accessAuditLogId;
	}
}