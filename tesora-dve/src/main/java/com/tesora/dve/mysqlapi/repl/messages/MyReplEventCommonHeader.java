// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl.messages;

public class MyReplEventCommonHeader {
	public long timestamp;
	public byte type;
	public long serverId;
	public long totalSize;
	public long masterLogPosition;
	public short flags;

	public MyReplEventCommonHeader() {
	}
	
	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public byte getType() {
		return type;
	}

	public void setType(byte type) {
		this.type = type;
	}

	public long getServerId() {
		return serverId;
	}

	public void setServerId(long serverId) {
		this.serverId = serverId;
	}

	public long getTotalSize() {
		return totalSize;
	}

	public void setTotalSize(long totalSize) {
		this.totalSize = totalSize;
	}

	public long getMasterLogPosition() {
		return masterLogPosition;
	}

	public void setMasterLogPosition(long masterLogPosition) {
		this.masterLogPosition = masterLogPosition;
	}

	public short getFlags() {
		return flags;
	}

	public void setFlags(short flags) {
		this.flags = flags;
	}
}