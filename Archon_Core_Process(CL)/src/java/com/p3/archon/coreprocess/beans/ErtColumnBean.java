package com.p3.archon.coreprocess.beans;

public class ErtColumnBean {

	private boolean selected;
	private boolean extOriginalCol;
	private String originalColName;
	private String expColName;
	private String colType;
	private boolean index;
	private boolean encrypt;

	public boolean isIndex() {
		return index;
	}

	public void setIndex(boolean index) {
		this.index = index;
	}

	public boolean isEncrypt() {
		return encrypt;
	}

	public void setEncrypt(boolean encrypt) {
		this.encrypt = encrypt;
	}


	public boolean isExtOriginalCol() {
		return extOriginalCol;
	}

	public void setExtOriginalCol(boolean extOriginalCol) {
		this.extOriginalCol = extOriginalCol;
	}

	public boolean isSelected() {
		return selected;
	}

	public void setSelected(boolean selected) {
		this.selected = selected;
	}

	public String getOriginalColName() {
		return originalColName;
	}

	public void setOriginalColName(String originalColName) {
		this.originalColName = originalColName;
	}

	public String getExpColName() {
		return expColName;
	}

	public void setExpColName(String expColName) {
		this.expColName = expColName;
	}

	public String getColType() {
		return colType;
	}

	public void setColType(String colType) {
		this.colType = colType;
	}

}
