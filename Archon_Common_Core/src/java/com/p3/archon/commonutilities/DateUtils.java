package com.p3.archon.commonutilities;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class DateUtils {

	final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	final static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public final static TimeZone TO_LOCAL_TZ = TimeZone.getDefault();
	public final static TimeZone TO_UTC_TZ = TimeZone.getTimeZone("UTC");

	public static Timestamp getCurrentSystemTime() {
		return new Timestamp(System.currentTimeMillis());
	}

	public static String timeDiff(long diff) {
		int diffDays = (int) (diff / (24 * 60 * 60 * 1000));
		String dateFormat = "";
		if (diffDays > 0) {
			dateFormat += diffDays + " days ";
			return dateFormat;
		}
		diff -= diffDays * (24 * 60 * 60 * 1000);

		int diffhours = (int) (diff / (60 * 60 * 1000));
		if (diffhours > 0) {
			dateFormat += leftNumPadding(diffhours, 2) + " hours ";
			return dateFormat;
		} else if (dateFormat.length() > 0) {
			dateFormat += "00 hours ";
		}
		diff -= diffhours * (60 * 60 * 1000);

		int diffmin = (int) (diff / (60 * 1000));
		if (diffmin > 0) {
			dateFormat += leftNumPadding(diffmin, 2) + " mins ";
			return dateFormat;
		} else if (dateFormat.length() > 0) {
			dateFormat += "00 mins ";
		}

		diff -= diffmin * (60 * 1000);

		int diffsec = (int) (diff / (1000));
		if (diffsec > 0) {
			dateFormat += leftNumPadding(diffsec, 2) + " secs ";
			return dateFormat;
		} else if (dateFormat.length() > 0) {
			dateFormat += "00 secs ";
		}
		return "few seconds";
	}

	private static String leftNumPadding(int str, int num) {
		return String.format("%0" + num + "d", str);
	}

	public static String getDateFromDBAsString(Timestamp ts) {
		if (ts == null)
			return "";
		String dateStr = ts.toString();
		if (dateStr.startsWith("1970") || dateStr.startsWith("1969"))
			return "";

		return dateStr.substring(0, 19);
	}
}