package com.tesora.dve.common;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.lang.reflect.Field;
import java.security.spec.KeySpec;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;

import com.tesora.dve.exceptions.PEException;

public final class PECryptoUtils {

	private static final CryptoSettings settings = buildSettings();

	private static CryptoSettings buildSettings() {
		Class<?> c = null;
		try {
			c = Class.forName("com.tesora.dve.common.DVEPrivateCryptoSettings");
		} catch (ClassNotFoundException cnfe) {
			c = DVEPublicCryptoSettings.class;
		}
		return new CryptoSettings(c);
	}


	private PECryptoUtils() {
	}

	private static Cipher createCrypter(int mode) throws Exception {
		// Create the key
		KeySpec keySpec = new PBEKeySpec(settings.getPassword().toCharArray(),
				settings.getSalt(),
				settings.getIterations());
		SecretKey key = SecretKeyFactory.getInstance("PBEWithMD5AndDES").generateSecret(keySpec);

		Cipher cipher = Cipher.getInstance(key.getAlgorithm());

		cipher.init(mode, key, new PBEParameterSpec(settings.getSalt(), settings.getIterations()));

		return cipher;
	}

	public static String encrypt(String str) throws PEException {
		if(StringUtils.isBlank(str))
			return str;

		try {
			Cipher cipher = createCrypter(Cipher.ENCRYPT_MODE);

			byte[] enc = cipher.doFinal(str.getBytes("UTF8"));

			return new String(Base64.encodeBase64(enc));
		} catch (Exception e) {
			throw new PEException("Failed to encrypt '" + str + "'", e);
		}
	}

	public static String decrypt(String str) throws PEException {
		if(StringUtils.isBlank(str))
			return str;

		try {
			Cipher cipher = createCrypter(Cipher.DECRYPT_MODE);

			return new String(cipher.doFinal(Base64.decodeBase64(str)), "UTF8");
		} catch (Exception e) {
			throw new PEException("Failed to decrypt '" + str + "'", e);
		}
	}

	public static void main(String[] args) {
		try {
			System.out.println(encrypt("password"));
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}
	
	private static final class CryptoSettings {

		private final int iterations;
		private final String password;
		private final byte[] salt;

		private final Throwable failed;
		private final Class<?> source;

		private CryptoSettings(Class<?> c) {
			this.source = c;
			int iterVal = 0;
			String pVal = null;
			byte[] sVal = null;
			Throwable any = null;
			try {
				Field iterField = c.getDeclaredField("iterations");
				Field pField = c.getDeclaredField("password");
				Field sField = c.getDeclaredField("salt");

				iterVal = (Integer)iterField.get(null);
				pVal = (String)pField.get(null);
				sVal = (byte[])sField.get(null);
			} catch (Throwable t) {
				any = t;
			}
			this.iterations = iterVal;
			this.password = pVal;
			this.salt = sVal;
			this.failed= any;
		}


		private final void fail() {
			if (failed != null)
				throw new IllegalStateException("Unable to obtain DVE crypto settings from " + source.getName());
		}

		final int getIterations() {
			fail();
			return iterations;
		}

		final String getPassword() {
			fail();
			return password;
		}

		final byte[] getSalt() {
			fail();
			return salt;
		}
	}
}
