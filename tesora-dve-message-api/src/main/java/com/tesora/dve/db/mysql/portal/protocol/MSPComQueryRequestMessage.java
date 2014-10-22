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
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;

public class MSPComQueryRequestMessage extends BaseMSPMessage {
    public static final MSPComQueryRequestMessage PROTOTYPE = new MSPComQueryRequestMessage();
    public static final byte TYPE_IDENTIFIER = (byte) 0x03;

    protected MSPComQueryRequestMessage() {
        super();
    }

    protected MSPComQueryRequestMessage(ByteBuf backing) {
        super(backing);
    }

    @Override
    public byte getMysqlMessageType() {
        return TYPE_IDENTIFIER;
    }

    @Override
    public MSPComQueryRequestMessage newPrototype(ByteBuf source) {
        source = source.slice();
        return new MSPComQueryRequestMessage(source);
    }

    public byte[] getQueryBytes() {
        return MysqlAPIUtils.unwrapOrCopyReadableBytes(getRemainingBuf());
    }

    public ByteBuf getQueryNative(){
        return getRemainingBuf();
    }

    private ByteBuf getRemainingBuf() {
        ByteBuf readBuf = readBuffer();
        ByteBuf remainingBuf = readBuf.slice(1,readBuf.readableBytes() - 1);//skip over type field.
        return remainingBuf;
    }

    public static MSPComQueryRequestMessage newMessage(String query, Charset encoding){
        return newMessage(query.getBytes(encoding) );
    }

	public static MSPComQueryRequestMessage newMessage(ByteBuffer rawQuery) {
		return newMessage(rawQuery.array());
	}

    public static MSPComQueryRequestMessage newMessage(byte[] rawQuery){
        ByteBuf buf = Unpooled.buffer().order(ByteOrder.LITTLE_ENDIAN).ensureWritable(rawQuery.length + 1);
        buf.writeByte(TYPE_IDENTIFIER);
        buf.writeBytes(rawQuery);
        return new MSPComQueryRequestMessage(buf);
    }

}
