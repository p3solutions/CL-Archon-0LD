package com.p3.archon.dboperations.dbmodel;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "ROLE_PRIVILEGE", indexes = { @Index(name = "rp_index", columnList = "ROLE_NAME") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class RolePrivilege {

	@Id
	@Column(name = "ROLE_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String roleName;

	@Column(name = "ROLE_DESC", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String roleDesc;

	@Column(name = "APP_PRIVILEGE", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String appPrivilege;

	@Column(name = "CONN_PRIVILEGE", nullable = true, columnDefinition = "varchar(255) DEFAULT NULL")
	String connPrivilege;

	public String getRoleName() {
		return roleName;
	}

	public void setRoleName(String roleName) {
		this.roleName = roleName;
	}

	public String getRoleDesc() {
		return roleDesc;
	}

	public void setRoleDesc(String roleDesc) {
		this.roleDesc = roleDesc;
	}

	public String getAppPrivilege() {
		return appPrivilege;
	}

	public void setAppPrivilege(String appPrivilege) {
		this.appPrivilege = appPrivilege;
	}

	public String getConnPrivilege() {
		return connPrivilege;
	}

	public void setConnPrivilege(String connPrivilege) {
		this.connPrivilege = connPrivilege;
	}

}
