package com.p3.archon.coreprocess.validations;

public class Validations {

	public static String checkValidFile(String name) {
		if (name != null)
			return getTextFormatted(name);
		return "unnamed";
	}

	public static String getTextFormatted(String string) {
		string = string.trim().replaceAll("[^_^\\p{Alnum}.]", "_").replace("^", "_").replaceAll("\\s+", "_");
		string = ((string.startsWith("_") && string.endsWith("_") && string.length() > 2)
				? string.substring(1).substring(0, string.length() - 2)
				: string);
		return string.length() > 0
				? ((string.charAt(0) >= '0' && string.charAt(0) <= '9') ? "_" : "") + string.toUpperCase()
				: string;
	}

	public static String checkValidFolder(String name) {
		if (name != null)
			return getFolderFormatted(name);
		return "unnamed";
	}

	public static String getFolderFormatted(String string) {
		string = string.trim().replaceAll("[^_^\\p{Alnum}.]", "_").replace("^", "_").replaceAll("\\s+", "_")
				.toUpperCase();
		string = ((string.startsWith("_") && string.endsWith("_") && string.length() > 2)
				? string.substring(1).substring(0, string.length() - 2)
				: string);
		return string;
	}

	public static String getFileFormatted(String string) {
		string = string.trim().replaceAll("[^_^\\p{Alnum}.]", "_").replace("^", "_").replaceAll("\\s+", "_");
		string = ((string.startsWith("_") && string.endsWith("_") && string.length() > 2)
				? string.substring(1).substring(0, string.length() - 2)
				: string);
		return string;
	}
}
