package com.p3.archon.dboperations.dbmodel.enums;

public enum UserAuthenticationType {

	RDBMS("Database Authentication"), SUPERADMIN("super admin mode"), BYPASS("By pass mode"), ANY("Any mode"), LDAP("LDAP Authentication");

	private String value;

	public String getValue() {
		return value;
	}

	UserAuthenticationType(String value) {
		this.value = value;
	}
	
	public static UserAuthenticationType getAuthType(String value) {
		if(value == null)
			return null;
		if(value.equals(UserAuthenticationType.RDBMS.getValue()))
			return UserAuthenticationType.RDBMS;
		else if(value.equals(UserAuthenticationType.LDAP.getValue()))
			return UserAuthenticationType.LDAP;
		else
			return null;
	}
}
