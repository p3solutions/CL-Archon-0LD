package com.p3.archon.dboperations.dbmodel;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@Embeddable
public class SshId implements Serializable {

	private static final long serialVersionUID = 1L;

	@Column(name = "mid", nullable = false, columnDefinition = "varchar(255)")
	private String mid;

	@Column(name = "sid", nullable = false, columnDefinition = "varchar(255)")
	private String sid;

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public String getMid() {
		return mid;
	}

	public void setMid(String mid) {
		this.mid = mid;
	}

	public String getSid() {
		return sid;
	}

	public void setSid(String sid) {
		this.sid = sid;
	}
}
