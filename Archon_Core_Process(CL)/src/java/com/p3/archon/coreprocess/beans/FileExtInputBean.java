package com.p3.archon.coreprocess.beans;

public class FileExtInputBean {

	private boolean fileNameExtOption = false; 
	private String fileNameOption = "";
	private String fileNamePrefixSeq = "";
	private String fileNameColumn = "";
	private String fileNameExtColumn = "";
	private String fileNameExtValue = "";
	private String blobColumn = ""; 
	private String tableName = "";
	private int fileNameStartSeq = 1;
	
	public boolean isFileNameExtOption() {
		return fileNameExtOption;
	}
	public void setFileNameExtOption(boolean fileNameExtOption) {
		this.fileNameExtOption = fileNameExtOption;
	}
	public String getFileNameOption() {
		return fileNameOption;
	}
	public void setFileNameOption(String fileNameOption) {
		this.fileNameOption = fileNameOption;
	}
	public String getFileNamePrefixSeq() {
		return fileNamePrefixSeq;
	}
	public void setFileNamePrefixSeq(String fileNamePrefixSeq) {
		this.fileNamePrefixSeq = fileNamePrefixSeq;
	}
	public String getFileNameColumn() {
		return fileNameColumn;
	}
	public void setFileNameColumn(String fileNameColumn) {
		this.fileNameColumn = fileNameColumn;
	}
	public String getFileNameExtColumn() {
		return fileNameExtColumn;
	}
	public void setFileNameExtColumn(String fileNameExtColumn) {
		this.fileNameExtColumn = fileNameExtColumn;
	}
	public String getFileNameExtValue() {
		return fileNameExtValue;
	}
	public void setFileNameExtValue(String fileNameExtValue) {
		this.fileNameExtValue = fileNameExtValue;
	}
	public String getBlobColumn() {
		return blobColumn;
	}
	public void setBlobColumn(String blobColumn) {
		this.blobColumn = blobColumn;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public int getFileNameStartSeq() {
		return fileNameStartSeq;
	}
	public void setFileNameStartSeq(int fileNameStartSeq) {
		this.fileNameStartSeq = fileNameStartSeq;
	}
	
	
	
	
}
