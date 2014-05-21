// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl;

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

import java.sql.SQLException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class MyReplErrorAdjudicator {
	Logger logger = Logger.getLogger(MyReplErrorAdjudicator.class);
	
	enum ReplSkipErrorOption {
		UNKNOWN("UNKNOWN"),
		ALL("ALL"),
		OFF("OFF")
		;

		private final String option;
		
		private ReplSkipErrorOption(String option) {
			this.option = option;
		}
		
		public String asString() {
			return option;
		}
		
		static public ReplSkipErrorOption fromString(String option) {
			for(ReplSkipErrorOption rseo : values()) {
				if (StringUtils.equalsIgnoreCase(rseo.asString(), option)) {
					return rseo;
				}
			}
			return UNKNOWN;
		}
	}
	
	boolean skipAll = false;
	boolean off = true;
	TreeSet<Integer> skipErrorList = new TreeSet<Integer>();

	public MyReplErrorAdjudicator() {
		skipAll = false;
		off = true;
		skipErrorList.clear();
	}
	
	public MyReplErrorAdjudicator(Set<String> skipErrors) {
		parseOptions(skipErrors);
		
		if (!isOff()) {
			if (skipAll()) {
				logger.warn("Replication Slave has been configured with option '" + ReplSkipErrorOption.ALL +  "'.  Any errors encountered during processing will be ignored and replication will continue.");
			} else {
				logger.warn("Replication Slave has been configured to ignore the following SQL error code(s): " + buildErrorCodeString());
				
			}
		}
	}
	
	public boolean skipAll() {
		return skipAll;
	}

	public boolean isOff() {
		return off;
	}

	public boolean validateErrorAndStop(int masterErrorCode) {
		return validateErrorAndStop(masterErrorCode, 0);
	}

	public boolean validateErrorAndStop(int masterErrorCode, Throwable slaveException) {
		if (skipAll()) {
			return false;
		}
		Throwable t = getRootException(slaveException);
		if (t instanceof SQLException) {
			return validateErrorAndStop(masterErrorCode, ((SQLException)t).getErrorCode());
		} else {
			return true;
		}
	}
	
	public boolean validateErrorAndStop(int masterErrorCode, int slaveErrorCode) {
		if (skipAll() || (masterErrorCode == slaveErrorCode))
			return false;
		
		if (isOff() && (masterErrorCode != slaveErrorCode))
			return true;
		
		return !skipErrorList.contains(masterErrorCode) || !skipErrorList.contains(slaveErrorCode); 
	}

	// for testing
	TreeSet<Integer> getSkipErrorList() {
		return skipErrorList;
	}
	
	private void parseOptions(Set<String> skipErrors) {
		if (skipErrors == null) {
			return;
		}
		
		skipErrorList.clear();
		skipErrorList.add(0);
		for(String errorCode : skipErrors) {
			try {
				off = false;
				skipAll = false;
				skipErrorList.add(Integer.parseInt(errorCode));
			} catch (NumberFormatException e) {
				// could be all or off
				switch(ReplSkipErrorOption.fromString(errorCode)) {
				case ALL:
					off = false;
					skipAll = true;
					skipErrorList.clear();
					return;
				
				case OFF:
				default: // reset to off with invalid option
					off = true;
					skipAll = false;
					skipErrorList.clear();
					return;
				
				}
			}
		}
	}

	private String buildErrorCodeString() {
		StringBuilder ret = new StringBuilder();
		
		boolean first = true;
		for(Integer code : skipErrorList) {
			if (!first) {
				ret.append(", ");
			}
			ret.append(code); 
			first = false;
		}
		
		return ret.toString();
	}
	
	private Throwable getRootException(Throwable exception) {
		Throwable root, lastEx = exception;
		for (root = exception; (root = root.getCause()) != null;) {
			lastEx = root;
		}
		return lastEx;
	}

}
