package com.tesora.dve.sql.transform.execution;

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

public class DMLExplainRecord {

	private final DMLExplainReason reason;
	private final String detail;
	private final long rowEstimate;
	
	public DMLExplainRecord(DMLExplainReason reason) {
		this(reason,null,-1);
	}
	
	public DMLExplainRecord(DMLExplainReason reason, String detail) {
		this(reason,detail,-1);
	}
	
	public DMLExplainRecord(DMLExplainReason reason, String detail, long rowEst) {
		this.reason = reason;
		this.detail = detail;
		rowEstimate = rowEst;
	}
	
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append(reason.getDescription());
		if (detail != null)
			buf.append(" ").append(detail);
		if (rowEstimate > -1)
			buf.append(" (max row estimate=").append(rowEstimate).append(")");
		return buf.toString();
	}
	
	public long getRowEstimate() {
		return rowEstimate;
	}
	
	public DMLExplainReason getReason() {
		return reason;
	}
	
	public DMLExplainRecord withRowEstimate(long rowEst) {
		return new DMLExplainRecord(reason,detail,rowEst);
	}
	
}
