package com.p3.archon.dboperations.dbmodel;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "MAIL_CONFIG")
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class MailConfig {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "ID", unique = true, nullable = false, columnDefinition = "int(1) NOT NULL")
	Integer id;

	@Column(name = "SMTP_SERVER", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String smtpserver;

	@Column(name = "SMTP_PORT", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String smtpport;

	@Column(name = "PASSWORD", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String password;

	@Column(name = "SMTP_FROM", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String smtpfrom;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getSmtpserver() {
		return smtpserver;
	}

	public void setSmtpserver(String smtpserver) {
		this.smtpserver = smtpserver;
	}

	public String getSmtpport() {
		return smtpport;
	}

	public void setSmtpport(String smtpport) {
		this.smtpport = smtpport;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getFrom() {
		return smtpfrom;
	}

	public void setFrom(String smtpfrom) {
		this.smtpfrom = smtpfrom;
	}
}
