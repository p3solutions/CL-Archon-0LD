package com.p3.archon.dboperations.dbmodel.enums;

public enum UserAduitAction {
	LOGIN("Login"), 
	LOGOUT("Logout"), 
	SESSION_INVALIDATED("Session invalidated"), 
	SESSION_TIMEOUT("Session Timeout"), 
	INVALID_LOGIN_ATTEMPT("Invalid login attempt"), 
	LOGIN_AT_LOCKDOWN("Login Attempt at Lockdown"), 
	LOGIN_WHEN_ACCOUNT_LOCKED("Login Attempt during Account Locked Period"), 
	SSO_LOGIN_FAILED("SSO authentication failed"),
	ACCOUNT_UNLOCK("Account Unlock"),
	PASSWORD_EXPIRED("Login Attempt after Password Expired"),
	LDAP_LOGIN_FAILED("LDAP Authentication Failed");

	UserAduitAction(String value) {
		this.value = value;
	}

	private String value;

	public String getValue() {
		return value;
	}

}
