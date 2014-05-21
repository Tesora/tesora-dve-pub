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

import org.apache.commons.lang.StringUtils;

public class MyBinLogPosition {
	private String masterHost;
	private String fileName;
	private long position;

	public MyBinLogPosition() {
	};

	public MyBinLogPosition(String masterHost, String fileName, long position) {
		this.masterHost = masterHost;
		this.fileName = fileName;
		this.position = position;
	}

	public String getMasterHost() {
		return masterHost;
	}

	public void setMasterHost(String masterHost) {
		this.masterHost = masterHost;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public long getPosition() {
		return position;
	}

	public void setPosition(long position) {
		this.position = position;
	}
	
	@Override
	public String toString() {
		return masterHost + "/" + fileName + "/" + position;
	}
	
	@Override
	public boolean equals(Object arg0) {
		if (arg0 instanceof MyBinLogPosition) {
			MyBinLogPosition other = (MyBinLogPosition)arg0;
			boolean ret = StringUtils.equals(this.masterHost, other.masterHost) && 
					StringUtils.equals(this.fileName, other.fileName) && 
					(this.position == other.position);
			return ret;
		} else {
			return super.equals(arg0);
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (int) (prime * result + (this.position^(this.position>>>32)));
		result = prime * result + ((masterHost == null) ? 0 : masterHost.hashCode());
		result = prime * result + ((fileName == null) ? 0 : fileName.hashCode());
		return result;
	}
}
