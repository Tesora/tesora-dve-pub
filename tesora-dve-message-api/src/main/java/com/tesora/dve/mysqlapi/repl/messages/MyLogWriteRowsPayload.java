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
import io.netty.buffer.Unpooled;

/**
 *
 */
public class MyLogWriteRowsPayload extends MyLogEventPacket {
    ByteBuf heapBuffer;

    public MyLogWriteRowsPayload(MyReplEventCommonHeader ch) {
        super(ch);
    }

    @Override
    public void accept(ReplicationVisitorTarget visitorTarget) throws PEException {
        visitorTarget.visit((MyLogWriteRowsPayload)this);
    }

    @Override
    public void marshallMessage(ByteBuf cb) {
        cb.writeBytes(heapBuffer.slice());
    }

    @Override
    public void unmarshallMessage(ByteBuf cb) throws PEException {
        this.heapBuffer = Unpooled.buffer(cb.readableBytes());
        this.heapBuffer.writeBytes(cb);
    }

    public String toString(){
        return String.format("MyLogWriteRowsPayload[header=%s, buffer=%s]",this.ch,heapBuffer);
    }
}
