// OS_STATUS: public
package com.tesora.dve.db.mysql.common;

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
