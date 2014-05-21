// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

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

import com.tesora.dve.db.mysql.MyFieldType;
import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

import java.nio.ByteOrder;

public class MyFieldPktResponse extends MyResponseMessage {
    public static final int CHARSET_OFFSET_RELATIVE_TO_EOF = -12;//delta applied to payload length to get to first fixed length type field.

    enum CacheState { NOT_CACHED, PACKED, UNPACKED}

    //variable length name fields.
	private String catalog = "def";
	private String database;
	private String table;
	private String orig_table;
	private String column;
	private String orig_column;

    //fixed length type fields.
	private byte charset;
	private Integer column_length;
	private MyFieldType column_type;
	private short flags = 0;
	private byte scale;
	private String defaultValue=null;

    CacheState state = CacheState.NOT_CACHED;
    ByteBuf cachedBuffer = Unpooled.EMPTY_BUFFER;

	@Override
	public void marshallMessage(ByteBuf cb) {
        if (state == CacheState.NOT_CACHED){
            ByteBuf newCache = Unpooled.buffer().order(ByteOrder.LITTLE_ENDIAN);
            fullPack(newCache);
            updateCache(newCache);
        }
        cb.writeBytes(cachedBuffer.slice());
	}

    public void fullPack(ByteBuf cb) {
        MysqlAPIUtils.putLengthCodedString(cb, catalog, true);
        MysqlAPIUtils.putLengthCodedString(cb, database, true);
        MysqlAPIUtils.putLengthCodedString(cb, table, true);
        MysqlAPIUtils.putLengthCodedString(cb, orig_table, true);
        MysqlAPIUtils.putLengthCodedString(cb, column, true);
        MysqlAPIUtils.putLengthCodedString(cb, orig_column, true);
        // The "spec" I used said this next byte was "filler", thru investigation
        // we determined that it is the number of bytes to the end of
        // the packet (not including the default). It seems to be always 12
        cb.writeByte((byte) 12);
        cb.writeByte(charset);
        cb.writeZero(1);
        cb.writeInt(column_length);
        cb.writeByte(column_type.getByteValue());
        cb.writeShort(flags);
        cb.writeByte(scale);
        cb.writeZero(2);
    }

    @Override
	public void unmarshallMessage(ByteBuf cb) {
        ByteBuf newCache = Unpooled.buffer(cb.readableBytes()).order(ByteOrder.LITTLE_ENDIAN);
        newCache.writeBytes( cb );

        updateCache(newCache);

        //parsing variable length name info is expensive, only unpack fixed length type info by default.
        unpackTypeInfo(cachedBuffer.slice());

        state = CacheState.PACKED;
	}

    protected void updateCache(ByteBuf newCache) {
        ByteBuf oldCache = cachedBuffer;
        cachedBuffer = newCache;
        ReferenceCountUtil.release(oldCache);
    }

    private void unpackTypeInfo(ByteBuf cb) {
        int remainingPayload = cb.readableBytes();
        int charsetOffset = remainingPayload + CHARSET_OFFSET_RELATIVE_TO_EOF;
        cb.skipBytes(charsetOffset);

        charset = cb.readByte();
        cb.skipBytes(1);
        column_length = cb.readInt();//TODO: this should be an unsigned int.  Will cause problems if length exceeds Integer.MAX_VALUE.
        column_type = MyFieldType.fromByte(cb.readByte());
        flags = cb.readShort();
        scale = cb.readByte();
        //plus two pad bytes of zero
    }

    private void unpackNameInfo(ByteBuf cb) {
        catalog = MysqlAPIUtils.getLengthCodedString(cb);
        database = MysqlAPIUtils.getLengthCodedString(cb);
        table = MysqlAPIUtils.getLengthCodedString(cb);
        orig_table = MysqlAPIUtils.getLengthCodedString(cb);
        column = MysqlAPIUtils.getLengthCodedString(cb);
        orig_column = MysqlAPIUtils.getLengthCodedString(cb);
        cb.skipBytes(1);
    }

    protected void ensureAllFieldsAreReadable(){
        if (state == CacheState.PACKED){
            //type info is unpacked by default, unpack the name info here.
            unpackNameInfo(cachedBuffer.slice());
            state = CacheState.UNPACKED;
        }
    }

    protected void ensureNotCached(){
        if (state == CacheState.NOT_CACHED)
            return;

        ensureAllFieldsAreReadable();//unpack any unread fields so we don't lose them.

        updateCache(Unpooled.EMPTY_BUFFER);
        state = CacheState.NOT_CACHED;
    }

    @Override
	public MyMessageType getMessageType() {
		return MyMessageType.FIELDPKT_RESPONSE;
	}

	public String getCatalog() {
        ensureAllFieldsAreReadable();
		return catalog;
	}

	public void setCatalog(String catalog) {
        ensureNotCached();
		this.catalog = catalog;
	}

	public String getDatabase() {
        ensureAllFieldsAreReadable();
		return database;
	}

	public void setDatabase(String database) {
        ensureNotCached();
		this.database = database;
	}

	public String getTable() {
        ensureAllFieldsAreReadable();
		return table;
	}

	public void setTable(String table) {
        ensureNotCached();
		this.table = table;
	}

	public String getOrig_table() {
        ensureAllFieldsAreReadable();
		return orig_table;
	}

	public void setOrig_table(String orig_table) {
        ensureNotCached();
		this.orig_table = orig_table;
	}

	public String getColumn() {
        ensureAllFieldsAreReadable();
		return column;
	}

	public void setColumn(String column) {
        ensureNotCached();
		this.column = column;
	}

	public String getOrig_column() {
        ensureAllFieldsAreReadable();
		return orig_column;
	}

	public void setOrig_column(String orig_column) {
        ensureNotCached();
		this.orig_column = orig_column;
	}

	public byte getCharset() {
        //fixed length type field, it is unpacked by default.
		return charset;
	}

	public void setCharset(byte serverCharset) {
        ensureNotCached();
		this.charset = serverCharset;
	}

	public int getColumn_length() {
        //fixed length type field, it is unpacked by default.
		return column_length;
	}

	public void setColumn_length(int column_length) {
        ensureNotCached();
		this.column_length = column_length;
	}

	public MyFieldType getColumn_type() {
        //fixed length type field, it is unpacked by default.
		return column_type;
	}

	public void setColumn_type(MyFieldType fieldTypeVarString) {
        ensureNotCached();
		this.column_type = fieldTypeVarString;
	}

	public short getFlags() {
        //fixed length type field, it is unpacked by default.
		return flags;
	}

	public void setFlags(short flags) {
        ensureNotCached();
		this.flags = flags;
	}

	public byte getScale() {
        //fixed length type field, it is unpacked by default.
		return scale;
	}

	public void setScale(byte scale) {
        ensureNotCached();
		this.scale = scale;
	}

	public void setFlags(Integer flags) {
        //delegate call, cache control handled there.
		setFlags( flags.shortValue() );
	}

	public void setScale(Integer scale) {
        //delegate call, cache control handled there.
		setScale(scale.byteValue());
	}

	public String getDefaultValue() {
        //not persisted field, no cache affect.
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
        //not a persisted field, no cache affect
		this.defaultValue = defaultValue;
	}

	public static class Factory {
		public MyFieldPktResponse newInstance() {
			return new MyFieldPktResponse();
		}
	}
}
