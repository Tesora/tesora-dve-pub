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
import com.tesora.dve.db.mysql.MysqlNativeType;
import com.tesora.dve.db.mysql.common.DBTypeBasedUtils;
import com.tesora.dve.db.mysql.common.DataTypeValueFunc;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class MyBinaryResultRowTest {
    ByteBuf rawRow;
    MyBinaryResultRow origRow;

    @Before
    public void setUp() throws Exception {
        rawRow = Unpooled.buffer().order(ByteOrder.LITTLE_ENDIAN);
        rawRow.writeZero(1);//bin row marker.
        MyNullBitmap bitmap = new MyNullBitmap(5, MyNullBitmap.BitmapType.RESULT_ROW);
        bitmap.setBit(2);//one based indexing, so second field is null
        rawRow.writeBytes(bitmap.getBitmapArray());

        List<DataTypeValueFunc> fieldConvertors = new ArrayList<>();

        {
            DataTypeValueFunc mysqlTypeFunc = DBTypeBasedUtils.getMysqlTypeFunc(MyFieldType.FIELD_TYPE_LONG);
            mysqlTypeFunc.writeObject(rawRow, 45);
            fieldConvertors.add(mysqlTypeFunc);
        }

        {
            DataTypeValueFunc mysqlTypeFunc = DBTypeBasedUtils.getMysqlTypeFunc(MyFieldType.FIELD_TYPE_LONG);
            //nothing in packet, this was null.
            fieldConvertors.add(mysqlTypeFunc);
        }

        {
            DataTypeValueFunc mysqlTypeFunc = DBTypeBasedUtils.getMysqlTypeFunc(MyFieldType.FIELD_TYPE_VARCHAR);
            mysqlTypeFunc.writeObject(rawRow, "one");
            fieldConvertors.add(mysqlTypeFunc);
        }

        {
            DataTypeValueFunc mysqlTypeFunc = DBTypeBasedUtils.getMysqlTypeFunc(MyFieldType.FIELD_TYPE_VARCHAR);
            mysqlTypeFunc.writeObject(rawRow, "two");
            fieldConvertors.add(mysqlTypeFunc);
        }

        {
            DataTypeValueFunc mysqlTypeFunc = DBTypeBasedUtils.getMysqlTypeFunc(MyFieldType.FIELD_TYPE_VARCHAR);
            mysqlTypeFunc.writeObject(rawRow, "three");
            fieldConvertors.add(mysqlTypeFunc);
        }

        origRow = new MyBinaryResultRow(fieldConvertors);
        origRow.unmarshallMessage(rawRow);
    }

    @Test
    public void testProjection_Identity() throws Exception {
        MyBinaryResultRow fullProj = origRow.projection(new int[]{0, 1, 2, 3, 4});

        //check that the sizes are the same.
        assertEquals(origRow.size(), fullProj.size());

        ByteBuf marshallProj = Unpooled.buffer().order(ByteOrder.LITTLE_ENDIAN);
        fullProj.marshallMessage(marshallProj);
        //check that the raw bytes are byte for byte equal.
        for (int i=0;i<rawRow.readableBytes();i++){
            assertEquals( "byte at index="+i, rawRow.getByte(i), marshallProj.getByte(i));
        }

        //check that all the slices are byte for byte equal.
        for (int i = 0; i < origRow.size(); i++) {
            assertEquals(origRow.getSlice(i), fullProj.getSlice(i));
        }

        //check that the decoded objects are equal
        for (int i = 0; i < origRow.size(); i++) {
            assertEquals(origRow.getValue(i), fullProj.getValue(i));
        }

    }

    @Test
    public void testProjection_Simple() throws Exception {
        {
            MyBinaryResultRow firstProj = origRow.projection(new int[]{0, 1});
            assertEquals(2, firstProj.size());
            assertEquals(45, firstProj.getValue(0));
            assertEquals(null, firstProj.getValue(1));
        }

        {
            MyBinaryResultRow secondProj = origRow.projection(new int[]{0, 3, 4});
            assertEquals(3, secondProj.size());
            assertEquals(45, secondProj.getValue(0));
            assertEquals("two", secondProj.getValue(1));
            assertEquals("three", secondProj.getValue(2));
        }
    }
}