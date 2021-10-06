package com.p3.archon.coreprocess.beans;

import java.util.List;

public class RecordData {

	private String data;
	private List<String> attachements;

	public RecordData(String data, List<String> attachements) {
		this.data = data;
		this.attachements = attachements;
	}

	public List<String> getAttachements() {
		return attachements;
	}

	public void setAttachements(List<String> attachements) {
		this.attachements = attachements;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}
}
