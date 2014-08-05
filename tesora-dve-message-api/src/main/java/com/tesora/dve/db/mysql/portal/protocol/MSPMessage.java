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

import com.tesora.dve.db.mysql.MysqlMessage;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;

/**
 *
 */
public interface MSPMessage extends MysqlMessage, ReferenceCounted {
    byte getMysqlMessageType();
    byte getSequenceID();
    void setSequenceID(byte sequence);
    MSPMessage newPrototype(byte sequenceID, ByteBuf source);



    ByteBuf unwrap();
    void writeTo(ByteBuf destination);
}
