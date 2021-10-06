package com.p3.archon.dboperations.dbmodel.enums;

public enum CreationOption {

	ARCHON(1), JOB_HANDLER(2);

	private int option;

	CreationOption(int option) {
		this.setOption(option);
	}

	public int getOption() {
		return option;
	}

	public void setOption(int option) {
		this.option = option;
	}

	public static CreationOption getValueOf(int n) {
		switch (n) {
		case 1:
			return CreationOption.ARCHON;
		case 2:
			return CreationOption.JOB_HANDLER;
		case 3:
			System.exit(0);
		default:
			System.out.println("Invalid Input ... try again ...");
			return null;
		}
	}
}
