package com.p3.archon.coreprocess.beans;

import java.util.LinkedHashSet;
import java.util.TreeMap;

public class ErtTableDetailsBean {
	
	public TreeMap<String, ErtTableCompleteDetailBean> tableDetails = new TreeMap<String, ErtTableCompleteDetailBean>();
	public String rpx = "100";
	public boolean ingestData = false;
	public String ingestionAppName = null;
	public boolean deleteData = false;
	public String mainTable = null;
	public LinkedHashSet<String> LinearSelectionOrder = new LinkedHashSet<String>();
	public String appName = null;
	public String holdingName = null;
	public String schemaName = null;
	public String username = null;
	public String password = null;
	public boolean showDateTime = false;

	public void clear() {
		tableDetails.clear();
		rpx = "100";
		ingestData = false;
		ingestionAppName = null;
		deleteData = false;
		mainTable = null;
		LinearSelectionOrder.clear();
		appName = null;
		holdingName = null;
		showDateTime = false;
	}
	
	public TreeMap<String, ErtTableCompleteDetailBean> getTableDetails() {
		return tableDetails;
	}

	public void setTableDetails(TreeMap<String, ErtTableCompleteDetailBean> tableDetails) {
		this.tableDetails = tableDetails;
	}

	public String getRpx() {
		return rpx;
	}

	public void setRpx(String rpx) {
		this.rpx = rpx;
	}

	public boolean isIngestData() {
		return ingestData;
	}

	public void setIngestData(boolean ingestData) {
		this.ingestData = ingestData;
	}

	public String getIngestionAppName() {
		return ingestionAppName;
	}

	public void setIngestionAppName(String ingestionAppName) {
		this.ingestionAppName = ingestionAppName;
	}

	public boolean isDeleteData() {
		return deleteData;
	}

	public void setDeleteData(boolean deleteData) {
		this.deleteData = deleteData;
	}

	public String getMainTable() {
		return mainTable;
	}

	public void setMainTable(String mainTable) {
		this.mainTable = mainTable;
	}

	public LinkedHashSet<String> getLinearSelectionOrder() {
		return LinearSelectionOrder;
	}

	public void setLinearSelectionOrder(LinkedHashSet<String> linearSelectionOrder) {
		LinearSelectionOrder = linearSelectionOrder;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getHoldingName() {
		return holdingName;
	}

	public void setHoldingName(String holdingName) {
		this.holdingName = holdingName;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public boolean isShowDateTime() {
		return showDateTime;
	}

	public void setShowDateTime(boolean showDateTime) {
		this.showDateTime = showDateTime;
	}

}