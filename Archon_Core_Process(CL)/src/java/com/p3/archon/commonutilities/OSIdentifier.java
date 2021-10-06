package com.p3.archon.commonutilities;

public class OSIdentifier {

	public static String osName = System.getProperty("os.name").toLowerCase();
	private static final String WIN = "win";
	private static final String MAC = "mac";
	private static final String NIX = "nix";
	private static final String NUX = "nux";
	private static final String AIX = "aix";
	private static final String SUN = "sunos";
	private static final String UNI = "unix";
	private static final String OTH = "others";

	public static boolean checkOS() {
		if (isWindows())
			return true;
		else
			return false;
	}

	public String getOSIdentifier() {
		if (isWindows()) {
			return WIN;
		} else if (isMac()) {
			return MAC;
		} else if (isUnix()) {
			return UNI;
		} else if (isSolaris()) {
			return SUN;
		} else {
			return OTH;
		}
	}

	public static boolean isWindows() {
		return (osName.indexOf(WIN) >= 0);
	}

	public static boolean isMac() {
		return (osName.indexOf(MAC) >= 0);
	}

	public static boolean isUnix() {
		return (osName.indexOf(NIX) >= 0 || osName.indexOf(NUX) >= 0 || osName.indexOf(AIX) > 0);
	}

	public static boolean isSolaris() {
		return (osName.indexOf(SUN) >= 0);
	}
}