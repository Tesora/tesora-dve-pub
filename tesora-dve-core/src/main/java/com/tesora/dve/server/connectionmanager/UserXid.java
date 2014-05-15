// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

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
