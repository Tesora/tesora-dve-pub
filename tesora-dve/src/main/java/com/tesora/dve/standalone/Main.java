// OS_STATUS: public
package com.tesora.dve.standalone;

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

import java.util.Properties;

import org.apache.log4j.Logger;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.server.bootstrap.BootstrapHost;

public final class Main {
	
	static Logger logger = Logger.getLogger(Main.class);
	
	public static void main(String[] args) {
		
		try {
			// Read the setup.properties file - which may not exist or be empty
			Properties setupProps;
			try {
				setupProps = PEFileUtils.loadPropertiesFromClasspath(Main.class, PEConstants.SERVER_FILE_NAME);
			} catch (Exception e) {
				setupProps = new Properties();
			}
			
			// Read the regular dve.properties file
			Properties props = PEFileUtils.loadPropertiesFile(Main.class, PEConstants.CONFIG_FILE_NAME);

			// Merge setupProps in with the dve.properites
			props.putAll(setupProps);

			logger.info("Starting DVE Server");
			
			BootstrapHost.startServices(Main.class, props);
			
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					try {
						BootstrapHost.stopServices();
					} catch (Exception e) {
						logger.error("Error stopping services - " + e.getMessage(), e);
					}
				}
			});
		} catch (Throwable e) {
			String msg1 = "DVE launcher has encountered an error and is stopping.";
			String msg2 = e.getMessage();

			logger.fatal(msg1);
			logger.fatal(msg2, e);

			System.err.println(msg1);
			System.err.println(msg2);
            System.err.flush();

			System.exit(1);
		}
	}

}
