package com.tesora.dve.mysqlapi.repl.messages;

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

import com.tesora.dve.db.mysql.libmy.MyMessage;
import io.netty.buffer.ByteBuf;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.MyMessageType;
import com.tesora.dve.exceptions.PEException;

public class MyReplEvent extends MyMessage implements ReplicationVisitorEvent {
	static final Logger logger = Logger.getLogger(MyReplEvent.class);

    ByteBuf rawPayload;

	MyReplEventCommonHeader commonHdr;
	MyLogEventPacket levp;
	
	public MyReplEvent() {
		super();
	};

	public MyReplEvent(MyReplEventCommonHeader ch) {
		this();
		this.commonHdr = ch;
	}

    @Override
    public void accept(ReplicationVisitorTarget visitorTarget) throws PEException {
        visitorTarget.visit((MyReplEvent)this);
    }

    @Override
    public MyMessageType getMessageType() {
        return MyMessageType.REPL_EVENT_RESPONSE;
    }

    @Override
    public boolean isMessageTypeEncoded() {
        return true;
    }

	@Override
	public void unmarshallMessage(ByteBuf cb) throws PEException {
        //assumes the provided buffer is already scoped to payload boundary.

        rawPayload = cb.slice().copy();


		// 19 bytes for the common header
		commonHdr = new MyReplEventCommonHeader();
		commonHdr.setTimestamp( cb.readUnsignedInt() );
		commonHdr.setType( cb.readByte() );
		commonHdr.setServerId(cb.readUnsignedInt() );
		commonHdr.setTotalSize(cb.readUnsignedInt() );
		commonHdr.setMasterLogPosition( cb.readUnsignedInt() );
		commonHdr.setFlags( cb.readShort() );

		// unmarshall message based on common header type
		if ( logger.isDebugEnabled() )
			logger.debug("Unmarshalling event of type: " + MyLogEventType.fromByte(commonHdr.getType()).name());

		levp = MyLogEventPacket.MySqlLogEventFactory
				.newInstance(MyLogEventType.fromByte(commonHdr.getType()), commonHdr);

		levp.unmarshallMessage(cb);
    }

    public ByteBuf getRawPayload(){
        return rawPayload;
    }

	@Override
    public void marshallMessage(ByteBuf cb) {
		cb.writeInt((int)commonHdr.getTimestamp());
		cb.writeByte(commonHdr.getType());
		cb.writeInt((int)commonHdr.getServerId());
		cb.writeInt((int)commonHdr.getTotalSize());
		cb.writeInt((int)commonHdr.getMasterLogPosition());
		cb.writeShort(commonHdr.getFlags());
		
		if (levp != null) {
			levp.marshallMessage(cb);
		}
	}

	public MyLogEventPacket getLogEventPacket() {
		return levp;
	}

	public void setLogEventPacket(MyLogEventPacket levp) {
		this.levp = levp;
	}

	public MyReplEventCommonHeader getCommonHeader() {
		return commonHdr;
	}
	
	@Override
	public String toString() {
		return "MyReplEvent: " + commonHdr.getMasterLogPosition();
	}


}
