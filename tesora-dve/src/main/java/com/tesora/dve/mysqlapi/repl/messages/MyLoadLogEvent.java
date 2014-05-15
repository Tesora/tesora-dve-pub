// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl.messages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.MyReplicationSlaveService;

public class MyLoadLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger
			.getLogger(MyLoadLogEvent.class);

	int threadId;
	int time;
	int ignoreLines;
	byte tableLen;
	byte dbLen;
	int columns;
	ByteBuf variableData; 
	
	public MyLoadLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		threadId = cb.readInt();
		time = cb.readInt();
		ignoreLines = cb.readInt();
		tableLen = cb.readByte();
		dbLen = cb.readByte();
		columns = cb.readInt();
		// TODO: need to parse out the variable part of the data
		variableData = Unpooled.buffer(cb.readableBytes());
		variableData.writeBytes(cb);
	}

	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		cb.writeInt(threadId);
		cb.writeInt(time);
		cb.writeInt(ignoreLines);
		cb.writeByte(tableLen);
		cb.writeByte(dbLen);
		cb.writeInt(columns);
		cb.writeBytes(variableData);
	}

	@Override
	public void processEvent(MyReplicationSlaveService plugin) {
		logger.warn("Message is parsed but no handler is implemented for log event type: LOAD_EVENT");
	}
}
