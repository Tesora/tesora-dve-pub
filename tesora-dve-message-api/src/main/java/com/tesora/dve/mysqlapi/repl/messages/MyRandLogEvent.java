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

import com.tesora.dve.exceptions.PEException;
import io.netty.buffer.ByteBuf;

import org.apache.log4j.Logger;

import com.google.common.primitives.UnsignedLong;

public class MyRandLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger
			.getLogger(MyRandLogEvent.class);

	UnsignedLong seed1;
	UnsignedLong seed2;
	
	public MyRandLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

    @Override
    public void accept(ReplicationVisitorTarget visitorTarget) throws PEException {
        visitorTarget.visit((MyRandLogEvent)this);
    }

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		seed1 = UnsignedLong.valueOf(cb.readLong());
		seed2 = UnsignedLong.valueOf(cb.readLong());
	}

	@Override
    public void marshallMessage(ByteBuf cb) {
		cb.writeLong(seed1.longValue());
		cb.writeLong(seed2.longValue());
	}

    public UnsignedLong getSeed1() {
        return seed1;
    }

    public UnsignedLong getSeed2() {
        return seed2;
    }
}
