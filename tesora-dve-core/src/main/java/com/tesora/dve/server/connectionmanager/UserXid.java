// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

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

public class UserXid {

	private String gtrid;
	private String bqual;
	private String formatID;
	
	public UserXid(String gtridValue, String bqualValue, String formatValue) {
		this.gtrid = gtridValue;
		this.bqual = bqualValue;
		this.formatID = formatValue;
	}

	public String getGlobalTxnID() {
		return gtrid;
	}
	
	public String getBranchTxnID() {
		return bqual;
	}
	
	public String getFormatID() {
		return formatID;
	}
	
	public String getSQL() {
		StringBuilder buf = new StringBuilder();
		buf.append(gtrid);
		if (bqual != null) {
			buf.append(",").append(bqual);
			if (formatID != null) 
				buf.append(",").append(formatID);
		}
		return buf.toString();
	}
}
