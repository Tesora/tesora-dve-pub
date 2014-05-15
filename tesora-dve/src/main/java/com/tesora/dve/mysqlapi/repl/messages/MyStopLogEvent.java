// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl.messages;

import io.netty.buffer.ByteBuf;

import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.MyReplicationSlaveService;

public class MyStopLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger
			.getLogger(MyStopLogEvent.class);

	public MyStopLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
	}

	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
	}

	@Override
	public void processEvent(MyReplicationSlaveService plugin) throws PEException {
		if ( logger.isDebugEnabled() ) 
			logger.debug("** Stop Event: NO BODY **");
		
		try {
			updateBinLogPosition(plugin);
		} catch (PEException e) {
			logger.error("Error updating binlog from Stop Log Event.", e);
			throw new PEException("Error updating bin log position",e);
		}
	}
}
