// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl.messages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.MyReplicationSlaveService;

public class MyCreateFileLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger
			.getLogger(MyCreateFileLogEvent.class);

	int threadId;
	ByteBuf variableData;
	
	public MyCreateFileLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		threadId = cb.readInt();
		// TODO: need to parse out the variable part of the data
		variableData = Unpooled.buffer(cb.readableBytes());
		variableData.writeBytes(cb);
	}

	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		cb.writeInt(threadId);
		cb.writeBytes(variableData);
	}

	@Override
	public void processEvent(MyReplicationSlaveService plugin) {
		logger.warn("Message is parsed but no handler is implemented for log event type: CREATE_FILE_EVENT");
	}
}
