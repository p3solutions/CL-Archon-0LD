package com.p3.archon.dboperations.dbmodel;

import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "USERS", indexes = { @Index(name = "users_index", columnList = "USER_ID"),
		@Index(name = "users_index1", columnList = "ROLE") })
@org.hibernate.annotations.Entity(
		dynamicUpdate = true
)
public class Users {

	@Id
	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userid;

	@Column(name = "ROLE", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String role;

	@Column(name = "NAME", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String name;

	@Column(name = "LAST_LOGIN_DT", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Date date;

	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

}
