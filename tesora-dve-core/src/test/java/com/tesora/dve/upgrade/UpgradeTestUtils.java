// OS_STATUS: public
package com.tesora.dve.upgrade;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import com.tesora.dve.exceptions.PEException;

public class UpgradeTestUtils {

	public static String[] getGoldVersion(int version) throws Throwable {
		return getFileContents(version,"catalog_version" + version + ".sql");
	}

	public static String[] getInfoSchema(int version) throws Throwable {
		return getFileContents(version,"infoschema_version" + version + ".sql");
	}
	
	private static String[] getFileContents(int version, String fileName) throws Throwable {
		InputStream is = UpgradeCodeNeededTest.class.getResourceAsStream(fileName);
		if (is == null)
			throw new PEException("Missing file " + fileName + " for catalog version " + version);
		final String cmdDelimeter = ";";

		ArrayList<String> out = new ArrayList<String>();
		
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(is));

			String line = null;
			while ((line = reader.readLine()) != null) {
				if (line.endsWith(cmdDelimeter)) {
					// strip off the separator
					out.add(line.substring(0,line.length() - 1));
				} else {
					throw new PEException("Malformed line from gold file: " + line);
				}
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			is.close();
			if (reader != null) try {
				reader.close();
			} catch (Exception e) {
				// ignore
			}
		}
		return out.toArray(new String[0]);		
		
	}
	
}
