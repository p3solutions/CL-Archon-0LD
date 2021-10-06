package com.p3.archon.dboperations.dbmodel;

import java.math.BigInteger;
import java.util.Date;

public class GroupedLoginCountDTO {

	private Date date;

	private BigInteger count;

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public BigInteger getCount() {
		return count;
	}

	public void setCount(BigInteger count) {
		this.count = count;
	}

}
