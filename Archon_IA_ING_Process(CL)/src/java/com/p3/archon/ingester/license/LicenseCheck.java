package com.p3.archon.ingester.license;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.NetworkInterface;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;

import javax.crypto.IllegalBlockSizeException;

import org.apache.commons.io.IOUtils;
import org.bouncycastle.openpgp.PGPException;

import com.p3.archon.dboperations.dbmodel.enums.CreationOption;
import com.p3.archon.securitymodule.SecurityModule;
import com.verhas.licensor.License;

public class LicenseCheck {

	private License lic = null;
	public static final String PUBRING = "pubring.gpg";
	public static final String SEC_DEFAULT_KEY = "Archon";
	public static final String ISSUE_FEATURE = "issue-date";
	public static final String VALID_FEATURE = "valid-date";
	public static final String MAC_CHECK = "maccheck";
	public static final String MAC = "mac";

	public void checkLicense(File licenseFileName) {
		lic = new License();

		try {
			InputStream is = getClass().getClassLoader().getResourceAsStream(PUBRING);
			lic.loadKeyRing(is, null);
		} catch (IOException e1) {
			System.out.println("License key is invalid. Probably file is tampered.");
			System.exit(0);
			return;
		}

		try {
			String licenseString = SecurityModule.perfromDecrypt(CreationOption.ARCHON.toString(), SEC_DEFAULT_KEY,
					IOUtils.toString(new FileReader(licenseFileName)));
			if (licenseString == null)
				throw new IllegalBlockSizeException();
			lic.setLicenseEncoded(licenseString);
		} catch (FileNotFoundException e) {
			System.out.println("License key unavailable. You will not be able to proceed.");
			System.exit(0);
			return;
		} catch (IOException e) {
			System.out.println("License key is invalid. Probably file is tampered.");
			System.exit(0);
			return;
		} catch (IllegalBlockSizeException e) {
			System.out.println("License key is invalid. Probably file is tampered.");
			System.exit(0);
			return;
		} catch (PGPException e) {
			System.out.println("License key is invalid/unavailable. You will not be able to proceed.");
			System.exit(0);
			return;
		}

		try {
			checkDateAndVersionValidity();
		} catch (NullPointerException e) {
			System.out.println("License key is invalid. Probably file is tampered.");
			System.exit(0);
		}
	}

	protected void checkDateAndVersionValidity() {
		String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date()).toString();

		String issueDate = lic.getFeature(ISSUE_FEATURE);
		if (!isLaterThan(today, issueDate)) {
			System.out.println("Issue date is too late, probably tampered system time");
			System.exit(0);
			return;
		}

		String validDate = lic.getFeature(VALID_FEATURE);
		if (validDate != null) {
			if (isLaterThan(today, validDate)) {
				System.out.println("License expired.");
				System.exit(0);
				return;
			}
		}

		String macc = lic.getFeature(MAC_CHECK);
		if (macc != null && macc.equals("true")) {
			List<String> mac = Arrays.asList(lic.getFeature(MAC).split(","));
			try {
				final Enumeration<NetworkInterface> inetAddresses = NetworkInterface.getNetworkInterfaces();
				final Collection<String> addresses = new LinkedList<String>();
				while (inetAddresses.hasMoreElements()) {
					final byte[] macBytes = inetAddresses.nextElement().getHardwareAddress();
					if (macBytes == null)
						continue;
					addresses.add(getMacAddress(macBytes));
				}
				addresses.retainAll(mac);
				if (addresses.size() == 0) {
					return;
				}
			} catch (Exception e) {
				return;
			}
		}
	}

	public static String getMacAddress(byte[] macBytes) {
		final StringBuilder strBuilder = new StringBuilder();
		for (int i = 0; i < macBytes.length; i++) {
			strBuilder.append(String.format("%02X%s", macBytes[i], (i < macBytes.length - 1) ? "-" : ""));
		}
		return strBuilder.toString().toUpperCase();
	}

	private boolean isLaterThan(String today, String compDate) {
		int x = today.compareTo(compDate);
		if (x >= 0)
			return true;
		return false;
	}
}
