package com.p3.archon.securitymodule;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

public class SecurityModule {

	// encryption
	public static final String CIPHER_INSTANCE = "AES/CBC/PKCS5PADDING";
	public static final String FORMAT = "UTF-8";
	public static final String ENCRYPTION_ENGINE = "AES";
	public static final String SEC_DEFAULT_KEY = "Archon";
	
	public static String encrypt(String key, String initVector, String value) {
		try {
			IvParameterSpec iv = new IvParameterSpec(initVector.getBytes(FORMAT));
			SecretKeySpec skeySpec = new SecretKeySpec(key.getBytes(FORMAT),
					ENCRYPTION_ENGINE);

			Cipher cipher = Cipher.getInstance(CIPHER_INSTANCE);
			cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);

			byte[] encrypted = cipher.doFinal(value.getBytes());
			return Base64.encodeBase64String(encrypted);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return null;
	}

	public static String decrypt(String key, String initVector, String encrypted) {
		try {
			IvParameterSpec iv = new IvParameterSpec(initVector.getBytes(FORMAT));
			SecretKeySpec skeySpec = new SecretKeySpec(key.getBytes(FORMAT),
					ENCRYPTION_ENGINE);

			Cipher cipher = Cipher.getInstance(CIPHER_INSTANCE);
			cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);

			byte[] original = cipher.doFinal(Base64.decodeBase64(encrypted));

			return new String(original);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return null;
	}

	public static String getSalt(String text) {
		while (text.length() < 16)
			text += text;
		return (text.length() > 16) ? text.substring(0, 16) : text;
	}

	public static String perfromEncrypt(String secKey, String saltkey, String value) {
		if (saltkey.equals(""))
			saltkey = SEC_DEFAULT_KEY;
		if (value.equals(""))
			return "";
		String key = getSalt(secKey);
		String initVector = getSalt(saltkey);
		return encrypt(key, initVector, value);
	}

	public static String perfromDecrypt(String secKey, String saltkey, String value) {
		if (saltkey.equals(""))
			saltkey = SEC_DEFAULT_KEY;
		if (value.equals(""))
			return "";
		String key = getSalt(secKey);
		String initVector = getSalt(saltkey);
		return decrypt(key, initVector, value);
	}
}
