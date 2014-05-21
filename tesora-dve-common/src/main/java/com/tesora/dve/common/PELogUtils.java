// OS_STATUS: public
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


import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;

public final class PELogUtils {
	private static final String MANIFEST_FILE_NAME = "META-INF/MANIFEST.MF";
	private static final String CORE_PROJECT_NAME = "tesora-dve-core";

	private static final String IMPL_BUILDTAG = "Implementation-BuildTag";
	private static final String IMPL_BUILDNUM = "Implementation-BuildNum";
	private static final String IMPL_BUILDID = "Implementation-BuildId";
	private static final String IMPL_VERSION = "Implementation-Version";

	private static final String BUILT_BY = "Built-By";
	private static final String BUILD_TIME = "Build-Time";

	private static final String attributeKeys[] = new String[] {
		IMPL_BUILDTAG, IMPL_BUILDNUM, IMPL_BUILDID, IMPL_VERSION, BUILT_BY, BUILD_TIME		
	};
	
	private static final Map<String,String> values = buildValues();
	
	private PELogUtils() {
	}
	
	// get individual values
	public static String getBuildTag() {
		return values.get(IMPL_BUILDTAG);
	}
	
	public static String getBuildNumber() {
		return values.get(IMPL_BUILDNUM);
	}
	
	public static String getBuildID() {
		return values.get(IMPL_BUILDID);
	}
	
	public static String getVersion() {
		return values.get(IMPL_VERSION);
	}
	
	public static String getBuildTime() {
		return values.get(BUILD_TIME);
	}
	
	public static String getBuilder() {
		return values.get(BUILT_BY);
	}
	
	public static String getBuildVersionString(boolean verbose) {
		StringBuffer ret;
		if (values.isEmpty())
			ret = new StringBuffer("Version not available");
		else {
			if (StringUtils.isBlank(getBuildNumber())) {
				ret = new StringBuffer("Developer Build");
				if (!StringUtils.isBlank(getBuilder())) {
					ret.append(" - ").append(getBuilder());
					if (!StringUtils.isBlank(getBuildTime()))
						ret.append(" at ").append(getBuildTime());
				}
			} else {
				ret = new StringBuffer("Version ").append(getVersion()).append("-")
						.append(getBuildNumber());

				if (verbose) {
					if (!StringUtils.isBlank(getBuilder())) {
						ret.append(" built by ").append(getBuilder());
						if (!StringUtils.isBlank(getBuildTime()))
							ret.append(" at ").append(getBuildTime());
					}
				}
			}
		}
		return ret.toString();
	}

	private static Attributes readManifestFile() throws PEException {
		try {
			Enumeration<URL> resources = PELogUtils.class.getClassLoader().getResources(MANIFEST_FILE_NAME);

			Attributes attrs = new Attributes();
			while (resources.hasMoreElements()) {
				URL url = resources.nextElement();
				if (url.toString().contains(CORE_PROJECT_NAME)) {
					Manifest manifest = new Manifest(url.openStream());
					attrs = manifest.getMainAttributes();
					break;
				}
			}
			return attrs;
		} catch (Exception e) {
			throw new PEException("Error retrieving build manifest", e);
		}
	}
	
	private static Map<String,String> buildValues() {
		Attributes attributes = null;
		try {
			attributes = readManifestFile();
		} catch (Exception e) {
			Logger.getLogger(PELogUtils.class).warn("Unable to read manifest file",e);
			attributes = null;
		}
		Map<String,String> out = new HashMap<String,String>();
		if (attributes != null) {
			for(String k : attributeKeys) {
				out.put(k, attributes.getValue(k));
			}
		}
		return out;
	}
}
