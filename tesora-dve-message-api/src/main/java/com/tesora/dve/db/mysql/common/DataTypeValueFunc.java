// OS_STATUS: public
package com.tesora.dve.db.mysql.common;

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
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;

public interface DataTypeValueFunc {

	public void writeObject(ByteBuf cb, Object value);

	public Object readObject(ByteBuf cb) throws PEException;
	
	public String getParamReplacement(Object value, boolean pstmt) throws PEException;
	
	public String getMysqlTypeName();

	public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException;
	
	Class<?> getJavaClass();
	
	public MyFieldType getMyFieldType();
}
