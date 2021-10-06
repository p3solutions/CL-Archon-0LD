package com.p3.archon.dboperations.dbmodel;

import java.math.BigDecimal;

public class GroupedUserRoleDTO {

	private String roleId;
	private BigDecimal count;

	public BigDecimal getCount() {
		return count;
	}

	public void setCount(BigDecimal count) {
		this.count = count;
	}

	public String getRoleId() {
		return roleId;
	}

	public void setRoleId(String roleId) {
		this.roleId = roleId;
	}

}