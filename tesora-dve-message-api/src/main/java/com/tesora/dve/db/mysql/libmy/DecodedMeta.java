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

import com.tesora.dve.db.mysql.MyFieldType;
import com.tesora.dve.db.mysql.MysqlNativeConstants;
import com.tesora.dve.db.mysql.common.DataTypeValueFunc;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;
import io.netty.buffer.ByteBuf;

/**
 *
 */
public class DecodedMeta implements DataTypeValueFunc {
    final MyFieldType decodedType; //TODO: this field is a hack to deal with the value funcs not holding the original decoded type.
    final DataTypeValueFunc valueFunc;
    final short flags;

    public DecodedMeta(DataTypeValueFunc valueFunc) {
        this(valueFunc.getMyFieldType(),valueFunc,(short)0);
    }

    public DecodedMeta(DataTypeValueFunc valueFunc, short flags) {
        this(valueFunc.getMyFieldType(),valueFunc,flags);
    }

    public DecodedMeta(MyFieldType decodedType, DataTypeValueFunc valueFunc, short flags) {
        this.decodedType = decodedType;
        this.valueFunc = valueFunc;
        this.flags = flags;
    }

    public boolean isUnsigned(){
        return ((int)flags & MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED) > 0;
    }

    public byte getNativeTypeForExecute(){
        if (this.decodedType == MyFieldType.FIELD_TYPE_INT24)
            return MyFieldType.FIELD_TYPE_LONG.getByteValue();
        else
            return this.decodedType.getByteValue();
    }

    @Override
    public void writeObject(ByteBuf cb, Object value) {
        valueFunc.writeObject(cb, value);
    }

    @Override
    public Object readObject(ByteBuf cb) throws PEException {
        return valueFunc.readObject(cb);
    }

    @Override
    public String getParamReplacement(Object value, boolean pstmt) throws PEException {
        return valueFunc.getParamReplacement(value, pstmt);
    }

    @Override
    public String getMysqlTypeName() {
        return valueFunc.getMysqlTypeName();
    }

    @Override
    public Object convertStringToObject(String value, ColumnMetadata colMd) throws PEException {
        return valueFunc.convertStringToObject(value, colMd);
    }

    @Override
    public Class<?> getJavaClass() {
        return valueFunc.getJavaClass();
    }

    @Override
    public MyFieldType getMyFieldType() {
        return decodedType;
    }
}
