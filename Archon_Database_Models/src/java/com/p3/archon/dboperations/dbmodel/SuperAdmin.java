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
@Table(name = "SUPER_ADMIN", indexes = { @Index(name = "superadmin_index", columnList = "USER_ID"),
		@Index(name = "superadmin_index1", columnList = "STATUS"),
		@Index(name = "superadmin_index2", columnList = "EMAIL"),
		@Index(name = "superadmin_index3", columnList = "ROLE") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class SuperAdmin {

	@GenericGenerator(name = "gen", strategy = "increment")
	@GeneratedValue(generator = "gen")
	@Column(name = "REQ_ID", unique = true, nullable = false, precision = 15, scale = 0)
	int reqId;

	@Id
	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userId;

	@Column(name = "USER_FIRST_NAME", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String firstName;

	@Column(name = "USER_LAST_NAME", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String lastName;

	@Column(name = "EMAIL", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String email;

	@Column(name = "PASSWORD", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String password;

	@Column(name = "SECRET_Q1", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String secretQ1;

	@Column(name = "SECRET_A1", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String secretA1;

	@Column(name = "SECRET_Q2", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String secretQ2;

	@Column(name = "SECRET_A2", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String secretA2;

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

	@Column(name = "HISTORY", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String passwordHistory;

	@Column(name = "LAST_CHANGED_DATE", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp lastPasswordChangedDate;

	@Column(name = "IS_ACCOUNT_LOCKED", nullable = false, columnDefinition = "TINYINT(1) DEFAULT 0")
	boolean accountLocked;

	public int getReqId() {
		return reqId;
	}

	public void setReqId(int reqId) {
		this.reqId = reqId;
	}

	public boolean isAccountLocked() {
		return accountLocked;
	}

	public void setAccountLocked(boolean accountLocked) {
		this.accountLocked = accountLocked;
	}

	public Timestamp getLastPasswordChangedDate() {
		return lastPasswordChangedDate;
	}

	public void setLastPasswordChangedDate(Timestamp lastPasswordChangedDate) {
		this.lastPasswordChangedDate = lastPasswordChangedDate;
	}

	public String getPasswordHistory() {
		return passwordHistory;
	}

	public void setPasswordHistory(String passwordHistory) {
		this.passwordHistory = passwordHistory;
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

	public String getUpdatedBy() {
		return updatedBy;
	}

	public void setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
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

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
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

	public String getBusinessJustification() {
		return businessJustification;
	}

	public void setBusinessJustification(String businessJustification) {
		this.businessJustification = businessJustification;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getSecretQ1() {
		return secretQ1;
	}

	public void setSecretQ1(String secretQ1) {
		this.secretQ1 = secretQ1;
	}

	public String getSecretQ2() {
		return secretQ2;
	}

	public void setSecretQ2(String secretQ2) {
		this.secretQ2 = secretQ2;
	}

	public String getSecretA1() {
		return secretA1;
	}

	public void setSecretA1(String secretA1) {
		this.secretA1 = secretA1;
	}

	public String getSecretA2() {
		return secretA2;
	}

	public void setSecretA2(String secretA2) {
		this.secretA2 = secretA2;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public UserStatusEnumList getStatus() {
		return status;
	}

	public void setStatus(UserStatusEnumList status) {
		this.status = status;
	}

}