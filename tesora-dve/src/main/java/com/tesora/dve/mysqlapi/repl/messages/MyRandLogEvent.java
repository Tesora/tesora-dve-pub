// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl.messages;

import io.netty.buffer.ByteBuf;

import org.apache.log4j.Logger;

import com.google.common.primitives.UnsignedLong;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.MyReplicationSlaveService;

public class MyRandLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger
			.getLogger(MyRandLogEvent.class);

	UnsignedLong seed1;
	UnsignedLong seed2;
	
	public MyRandLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		seed1 = UnsignedLong.valueOf(cb.readLong());
		seed2 = UnsignedLong.valueOf(cb.readLong());
	}

	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		cb.writeLong(seed1.longValue());
		cb.writeLong(seed2.longValue());
	}

	@Override
	public void processEvent(MyReplicationSlaveService plugin) {
		if (logger.isDebugEnabled()) {
			logger.debug("** START Rand Event **");
			logger.debug("seed1="+seed1.toString());
			logger.debug("seed2="+seed2.toString());
			logger.debug("** END Rand Event **");
		}
		plugin.getSessionVariableCache().setRandValue(seed1, seed2);
	}
}
