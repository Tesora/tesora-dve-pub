package com.tesora.dve.db.mysql.portal.protocol;

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

import io.netty.buffer.ByteBuf;

public class MSPComSetOptionRequestMessage extends BaseMSPMessage<Short> {
    static final int INDEX_OF_OPTIONFLAG = 0;

    public MSPComSetOptionRequestMessage() {
        super();
    }

    public MSPComSetOptionRequestMessage(byte sequenceID, ByteBuf backing) {
        super(sequenceID, backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return (byte) 0x1b;
    }

    @Override
    protected Short unmarshall(ByteBuf source) {
        return source.getShort(INDEX_OF_OPTIONFLAG);
    }

    @Override
    public MSPComSetOptionRequestMessage newPrototype(byte sequenceID, ByteBuf source) {
        return new MSPComSetOptionRequestMessage(sequenceID,source);
    }

    public short getOptionFlag() {
        return readState();
    }


}
