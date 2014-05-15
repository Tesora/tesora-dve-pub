// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl.messages;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import com.tesora.dve.db.mysql.libmy.MyMessageType;
import com.tesora.dve.db.mysql.libmy.MyRequestMessage;
import com.tesora.dve.exceptions.PEException;

public class MyComBinLogDumpRequest extends MyRequestMessage {

	long binlogPosition;
	int slaveServerID;
	String binlogFileName=null;

	public MyComBinLogDumpRequest() {}

	public MyComBinLogDumpRequest(int slaveServerId, long binlogPosition, String binlogFileName) {
		this.slaveServerID = slaveServerId;
		this.binlogPosition = binlogPosition;
		this.binlogFileName = binlogFileName;
	}
	
	public long getBinlogPosition() {
		return binlogPosition;
	}

	public int getSlaveServerID() {
		return slaveServerID;
	}

	public String getBinlogFileName() {
		return binlogFileName;
	}

	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		cb.writeInt((int) binlogPosition);
		cb.writeZero(2); 	// binlog_flags
		cb.writeInt(slaveServerID);
		if ( binlogFileName != null ) {
			cb.writeBytes(binlogFileName.getBytes(CharsetUtil.UTF_8));
		}
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		binlogPosition = cb.readUnsignedInt();
		cb.skipBytes(2);
		slaveServerID = cb.readInt();
		if ( cb.readableBytes() > 0 ) {
			binlogFileName = cb.readSlice(cb.readableBytes()).toString(CharsetUtil.UTF_8);
		}
	}

	@Override
	public MyMessageType getMessageType() {
		return MyMessageType.COM_BINLOG_DUMP_REQUEST;
	}
}
