package com.p3.archon.coreprocess.beans;

import java.util.TreeMap;

public class UnSupportedCharList {

	public static TreeMap<String,UnSupportedCharBean> unSupportCharList = new TreeMap<String,UnSupportedCharBean>();

	public static TreeMap<String, UnSupportedCharBean> getUnSupportCharList() {
		return unSupportCharList;
	}

	public static void setUnSupportCharList(TreeMap<String, UnSupportedCharBean> unSupportCharList) {
		UnSupportedCharList.unSupportCharList = unSupportCharList;
	}


}
