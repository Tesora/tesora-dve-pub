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
import io.netty.util.CharsetUtil;

import com.tesora.dve.exceptions.PECodingException;

public class MyLoadDataResponse extends MyResponseMessage {

	String fileName;
	
	public MyLoadDataResponse(String fileName) {
		this.fileName = fileName;
	}
	
	@Override
    public void marshallMessage(ByteBuf cb) {
		cb.writeByte(0xFB);
		cb.writeBytes(fileName.getBytes(CharsetUtil.UTF_8));
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		throw new PECodingException(getClass().getSimpleName());
	}

	@Override
	public MyMessageType getMessageType() {
		return MyMessageType.LOCAL_INFILE_DATA;
	}

}
