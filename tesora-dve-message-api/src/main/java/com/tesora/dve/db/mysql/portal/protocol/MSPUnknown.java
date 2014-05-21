// OS_STATUS: public
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

public class MSPUnknown extends BaseMSPMessage {
    byte messageType;

    public MSPUnknown() {
        super();
        this.messageType = (byte)0xFF;
    }

    public MSPUnknown(byte messageType, byte sequenceID, ByteBuf backing) {
        super(sequenceID, backing);
        this.messageType = messageType;
    }

    public MSPUnknown(byte sequenceID, ByteBuf backing) {
        this((byte) 0xFF, sequenceID, backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return messageType;
    }

    @Override
    public MSPUnknown newPrototype(byte sequenceID, ByteBuf source) {
        return new MSPUnknown(sequenceID,source);
    }
}
