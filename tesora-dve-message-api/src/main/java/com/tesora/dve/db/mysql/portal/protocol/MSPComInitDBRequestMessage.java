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

import java.nio.charset.Charset;

public class MSPComInitDBRequestMessage extends BaseMSPMessage<String> {
    public static final MSPComInitDBRequestMessage PROTOTYPE = new MSPComInitDBRequestMessage();
    Charset decodingCharset;


    protected MSPComInitDBRequestMessage() {
        super();
    }

    protected MSPComInitDBRequestMessage(ByteBuf backing) {
        super(backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return (byte) 0x02;
    }

    @Override
    public MSPComInitDBRequestMessage newPrototype(ByteBuf source) {
        source = source.slice();
        return new MSPComInitDBRequestMessage(source);
    }

    public void setDecodingCharset(Charset javaCharset){
        this.decodingCharset = javaCharset;
    }

    public String getInitialDatabase() {
        return readState();
    }

    @Override
    protected String unmarshall(ByteBuf source) {
        if (decodingCharset == null)
            throw new IllegalStateException("initDB request cannot unmarshall a packet without a decoding charset.");
        source.skipBytes(1);//skip type field.
        return source.toString(decodingCharset);
    }

    @Override
    protected void marshall(String state, ByteBuf destination) {
        throw new UnsupportedOperationException();
    }

}
