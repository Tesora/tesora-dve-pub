// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl.messages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.MyReplicationSlaveService;

public class MyAppendBlockLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger
			.getLogger(MyAppendBlockLogEvent.class);

	int fileId;
	ByteBuf dataBlock;
	
	public MyAppendBlockLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		fileId = cb.readInt();
		dataBlock = Unpooled.buffer(cb.readableBytes());
		dataBlock.writeBytes(cb);
	}

	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		cb.writeInt(fileId);
		cb.writeBytes(dataBlock);
	}

	@Override
	public void processEvent(MyReplicationSlaveService plugin) throws PEException {
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("** START AppendBlock Log Event **");
				logger.debug("File id = " + fileId + ", size of block = " + dataBlock.readableBytes());
				logger.debug("** END AppendBlock Log Event **");
			}

			plugin.getInfileHandler().addBlock(fileId, dataBlock.array());
			
			updateBinLogPosition(plugin);

		} catch (IOException e) {
			throw new PEException("Received APPEND_BLOCK_EVENT but cannot add to infile.", e);
		}
	}
}
