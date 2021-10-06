package com.p3.archon.dboperations.dbmodel;

public class GroupedStatisticsRoleDTO {

	private String item;
	private Long count;

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	public String getItem() {
		return item;
	}

	public void setItem(String item) {
		this.item = item;
	}

}