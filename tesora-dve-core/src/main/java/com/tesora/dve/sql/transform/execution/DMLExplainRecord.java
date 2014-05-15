// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

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
