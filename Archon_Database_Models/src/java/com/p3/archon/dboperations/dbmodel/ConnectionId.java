package com.p3.archon.dboperations.dbmodel;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@SuppressWarnings({ "serial" })
@Embeddable
public class ConnectionId implements Serializable{

	@Column(name = "WF_ID")
	String wfId;

	@Column(name = "USER_ID")
	String loggedUser;

	public String getWfId() {
		return wfId;
	}

	public void setWfId(String wfId) {
		this.wfId = wfId;
	}

	public String getLoggedUser() {
		return loggedUser;
	}

	public void setLoggedUser(String loggedUser) {
		this.loggedUser = loggedUser;
	}

}
