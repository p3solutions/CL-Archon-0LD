package com.p3.archon.coreprocess.beans;

import java.util.ArrayList;

public class ErtTableCompleteDetailBean {
	
	private boolean selected;
	private boolean deleteData;
	private String aliasName;
	private String queryClause;
	private String whereOrderClause;
	private ArrayList<ErtColumnBean> columns;
	private ArrayList<String> relatedTables;
	private String relationsJSON;
	private String TableJoinCond;
	private String TableJoinRelCond;
	private String KeyColumns;
	
	public boolean isSelected() {
		return selected;
	}
	public void setSelected(boolean selected) {
		this.selected = selected;
	}
	public boolean isDeleteData() {
		return deleteData;
	}
	public void setDeleteData(boolean deleteData) {
		this.deleteData = deleteData;
	}

	public String getWhereOrderClause() {
		return whereOrderClause;
	}
	public void setWhereOrderClause(String whereOrderClause) {
		this.whereOrderClause = whereOrderClause;
	}
	public ArrayList<ErtColumnBean> getColumns() {
		return columns;
	}
	public void setColumns(ArrayList<ErtColumnBean> columns) {
		this.columns = columns;
	}
	public String getQueryClause() {
		return queryClause;
	}
	public void setQueryClause(String queryClause) {
		this.queryClause = queryClause;
	}
	public String getAliasName() {
		return aliasName;
	}
	public void setAliasName(String aliasName) {
		this.aliasName = aliasName;
	}
	public ArrayList<String> getRelatedTables() {
		return relatedTables;
	}
	public void setRelatedTables(ArrayList<String> relatedTables) {
		this.relatedTables = relatedTables;
	}
	public String getRelationsJSON() {
		return relationsJSON;
	}
	public void setRelationsJSON(String relationsJSON) {
		this.relationsJSON = relationsJSON;
	}
	public String getTableJoinCond() {
		return TableJoinCond;
	}
	public void setTableJoinCond(String tableJoinCond) {
		TableJoinCond = tableJoinCond;
	}
	public String getTableJoinRelCond() {
		return TableJoinRelCond;
	}
	public void setTableJoinRelCond(String tableJoinRelCond) {
		TableJoinRelCond = tableJoinRelCond;
	}
	public String getKeyColumns() {
		return KeyColumns;
	}
	public void setKeyColumns(String keyColumns) {
		KeyColumns = keyColumns;
	}
	
}
