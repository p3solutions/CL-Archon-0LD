package com.p3.archon.dboperations.dbmodel;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "PRIVILEGES_DEFINITION", indexes = { @Index(name = "pd_index", columnList = "PRIVILEGE_ID") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class Privileges {

	@Id
	@Column(name = "PRIVILEGE_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String privilege;

	@Column(name = "PRIVILEGE_TYPE", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String privType;

	@Column(name = "DESCRIPTION", nullable = false, columnDefinition = "varchar(500) NOT NULL")
	String privDesc;

	public String getPrivilege() {
		return privilege;
	}

	public void setPrivilege(String privilege) {
		this.privilege = privilege;
	}

	public String getPrivType() {
		return privType;
	}

	public void setPrivType(String privType) {
		this.privType = privType;
	}

	public String getPrivDesc() {
		return privDesc;
	}

	public void setPrivDesc(String privDesc) {
		this.privDesc = privDesc;
	}

}
