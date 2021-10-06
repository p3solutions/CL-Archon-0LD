package com.p3.archon.dboperations.dbmodel;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Index;
import javax.persistence.Table;

import com.p3.archon.dboperations.dbmodel.enums.PidStatus;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "PID_DETAILS", indexes = { @Index(name = "pid_details_index", columnList = "JOB_ID"),
		@Index(name = "pid_details_index1", columnList = "RUN_ID"),
		@Index(name = "pid_details_index2", columnList = "JOB_ATTEMPT"),
		@Index(name = "pid_details_index3", columnList = "STATUS") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class PidDetails implements Serializable {

	private static final long serialVersionUID = 1L;

	@EmbeddedId
	RowId rowId;

	@Column(name = "PID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String pId;

	@Column(name = "HANDLER_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String handlerName;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "STATUS", nullable = false, columnDefinition = "int(11) DEFAULT 0")
	PidStatus status;

	@Column(name = "USER_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String user;

	@Column(name = "MESSAGE", nullable = true, columnDefinition = "varchar(10000) DEFAULT NULL")
	String message;

	public String getHandlerName() {
		return handlerName;
	}

	public void setHandlerName(String handlerName) {
		this.handlerName = handlerName;
	}

	public RowId getRowId() {
		return rowId;
	}

	public void setRowId(RowId rowId) {
		this.rowId = rowId;
	}

	public PidStatus getStatus() {
		return status;
	}

	public void setStatus(PidStatus status) {
		this.status = status;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getpId() {
		return pId;
	}

	public void setpId(String pId) {
		this.pId = pId;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

}