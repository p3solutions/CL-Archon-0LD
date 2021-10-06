package com.p3.archon.dboperations.dbmodel;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;

import com.p3.archon.dboperations.dbmodel.enums.UserStatusEnumList;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "LDAP_USER_DETAILS", indexes = { @Index(name = "ldap_user_details_index", columnList = "REQ_ID"),
		@Index(name = "ldap_user_details_index1", columnList = "USER_ID"),
		@Index(name = "ldap_user_details_index2", columnList = "STATUS"),
		@Index(name = "ldap_user_details_index3", columnList = "EMAIL"),
		@Index(name = "ldap_user_details_index4", columnList = "ROLE") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class LdapUserDetails {

	@Id
	@GenericGenerator(name = "gen", strategy = "increment")
	@GeneratedValue(generator = "gen")
	@Column(name = "REQ_ID", unique = true, nullable = false, precision = 15, scale = 0)
	int reqId;

	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userId;

	@Column(name = "USER_FIRST_NAME", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String firstName;

	@Column(name = "USER_LAST_NAME", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String lastName;

	@Column(name = "EMAIL", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String email;

	@Column(name = "ARCHON_ALERT_EMAIL", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String archonAlertEmail;

	@Column(name = "PASSWORD", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String password;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "STATUS", nullable = true, columnDefinition = "int(11) DEFAULT NULL")
	UserStatusEnumList status;

	@Column(name = "ROLE", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String role;

	@Column(name = "BUSINESS_JUSTIFICATION", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String businessJustification;

	@Column(name = "COMMENT", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String comment;

	@Column(name = "REGISTERED_DATE", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp registeredDate;

	@Column(name = "LASTACCESS_DATE", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp lastAccessDate;

	@Column(name = "UPDATED_BY", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String updatedBy;

	@Column(name = "ATTEMPTS", nullable = true, columnDefinition = "int(11) DEFAULT 0")
	int attempts;

	@Column(name = "LOCK_DOWN", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp lockdown;

	@Column(name = "IS_ACCOUNT_LOCKED", nullable = false, columnDefinition = "TINYINT(1) DEFAULT 0")
	boolean accountLocked;

	@Column(name = "IS_ALERT_SENT_TO_ARCHON_NOTIFICATION_MAIL", nullable = false, columnDefinition = "TINYINT(1) DEFAULT 0")
	boolean alertSentToArchonNotificationMail;

	public int getReqId() {
		return reqId;
	}

	public void setReqId(int reqId) {
		this.reqId = reqId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getArchonAlertEmail() {
		return archonAlertEmail;
	}

	public void setArchonAlertEmail(String archonAlertEmail) {
		this.archonAlertEmail = archonAlertEmail;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public UserStatusEnumList getStatus() {
		return status;
	}

	public void setStatus(UserStatusEnumList status) {
		this.status = status;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public String getBusinessJustification() {
		return businessJustification;
	}

	public void setBusinessJustification(String businessJustification) {
		this.businessJustification = businessJustification;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public Timestamp getRegisteredDate() {
		return registeredDate;
	}

	public void setRegisteredDate(Timestamp registeredDate) {
		this.registeredDate = registeredDate;
	}

	public Timestamp getLastAccessDate() {
		return lastAccessDate;
	}

	public void setLastAccessDate(Timestamp lastAccessDate) {
		this.lastAccessDate = lastAccessDate;
	}

	public String getUpdatedBy() {
		return updatedBy;
	}

	public void setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
	}

	public int getAttempts() {
		return attempts;
	}

	public void setAttempts(int attempts) {
		this.attempts = attempts;
	}

	public Timestamp getLockdown() {
		return lockdown;
	}

	public void setLockdown(Timestamp lockdown) {
		this.lockdown = lockdown;
	}

	public boolean isAccountLocked() {
		return accountLocked;
	}

	public void setAccountLocked(boolean accountLocked) {
		this.accountLocked = accountLocked;
	}

	public boolean isAlertSentToArchonNotificationMail() {
		return alertSentToArchonNotificationMail;
	}

	public void setAlertSentToArchonNotificationMail(boolean alertSentToArchonNotificationMail) {
		this.alertSentToArchonNotificationMail = alertSentToArchonNotificationMail;
	}

}