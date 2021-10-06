package com.p3.archon.dboperations.dbmodel.enums;

public enum ServerType {
	DEV(1, "dev"), TEST(2, "test"), STAGE(3, "stage"), PROD(4, "prod"), OTHERS(5, "others");

	private int key;
	private String value;

	private ServerType(int key, String value) {
		this.setKey(key);
		this.setValue(value);
	}

	public static String getServerType(int key) {
		for (ServerType x : ServerType.values())
			if (x.getKey() == key)
				return x.getValue();
		return "others";
	}

	public static ServerType getServerEnumValue(int key) {
		for (ServerType x : ServerType.values())
			if (x.getKey() == key)
				return x;
		return OTHERS;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

}
