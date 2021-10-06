package com.p3.archon.dboperations.dbmodel;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Index;
import javax.persistence.Table;

import com.p3.archon.dboperations.dbmodel.enums.UserAuthenticationType;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "SS_HOST", indexes = { @Index(name = "ssh_index", columnList = "mid"),
		@Index(name = "ssh_index1", columnList = "sid") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class Sshost implements Serializable {

	private static final long serialVersionUID = 1L;

	@EmbeddedId
	SshId sshId;

	@Column(name = "uid", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String uid;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "AUTHENTICATION_TYPE", nullable = false)
	private UserAuthenticationType authType = UserAuthenticationType.RDBMS;

	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userId;

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public UserAuthenticationType getAuthType() {
		return authType;
	}

	public void setAuthType(UserAuthenticationType authType) {
		this.authType = authType;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public SshId getSshId() {
		return sshId;
	}

	public void setSshId(SshId sshId) {
		this.sshId = sshId;
	}

}
