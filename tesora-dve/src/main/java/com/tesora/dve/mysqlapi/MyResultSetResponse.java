// OS_STATUS: public
package com.tesora.dve.mysqlapi;

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

import com.tesora.dve.db.mysql.libmy.*;
import io.netty.buffer.ByteBuf;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.db.mysql.MyFieldType;
import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.db.mysql.MysqlNativeConstants;
import com.tesora.dve.db.mysql.MysqlNativeType;
import com.tesora.dve.db.mysql.MysqlNativeType.MysqlType;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;

public class MyResultSetResponse extends MyResponseMessage {
	protected List<MyResponseMessage> resultSetPackets = new ArrayList<MyResponseMessage>();
	protected long fieldCount;
	protected int nextPacketNumber = 1;
	protected DBNative dbNative;
	protected ResultSet rs;
	protected ResultSetMetaData rsmd;
	private ColumnSet columnSet;

	protected MyResultSetResponse() {
		setPacketNumber(1);
	}
	
	public MyResultSetResponse( DBNative dbNative, ResultSetMetaData rsmd, ColumnSet columnSet, ResultSet rs) throws PEException {
		this();
		this.dbNative = dbNative;
		this.rs = rs;
		this.rsmd = rsmd;
		this.columnSet = columnSet;
		createFieldPackets(new MyFieldPktResponse.Factory());
		createRowDataPackets();
	}
	
	protected void addPacket(MyResponseMessage rm) {
		rm.setPacketNumber(++nextPacketNumber);
		resultSetPackets.add(rm);
	}

	protected void createFieldPackets(MyFieldPktResponse.Factory responseFactory) throws PEException {
		try {
			setFieldCount(rsmd.getColumnCount());

			// create/populate FieldPacket (one for each column in
			// result set)
			MysqlNativeType colNativeType;
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				colNativeType = (MysqlNativeType) dbNative.findType(
						rsmd.getColumnTypeName(i));

				MyFieldPktResponse fp = responseFactory.newInstance();
				fp.setDatabase(rsmd.getCatalogName(i));
				fp.setOrig_table(rsmd.getTableName(i));
				fp.setTable(rsmd.getTableName(i));
				fp.setOrig_column(rsmd.getColumnName(i));
				fp.setColumn(rsmd.getColumnLabel(i));
				fp.setColumn_length(rsmd.getColumnDisplaySize(i));
				fp.setColumn_type(MyFieldType.mapFromNativeType(MysqlType.toMysqlType(rsmd.getColumnTypeName(i))));
				fp.setCharset(colNativeType.getCharSet());
				fp.setFlags(colNativeType.getFieldTypeFlags());
				fp.setScale(rsmd.getScale(i));

				if ((colNativeType.isUnsignedAttribute() && !rsmd.isSigned(i))
						|| colNativeType.getTypeName().equals("bit")) // hack to mimic Mysql behaviour
					fp.setFlags(fp.getFlags() + MysqlNativeConstants.FLDPKT_FLAG_UNSIGNED);

				addPacket(fp);
				// PROTOCONV
				// if ( ((PEResultSetMetaData) rsmd).hasDefault(i) )
				// fp.setDefaultValue(((PEResultSetMetaData)
				// rsmd).getDefaultValue(i));

			}
		} catch (SQLException sqle) {
			throw new PEException(sqle);
		}
		// add EOF Packet
		addEOFPacket(2, 0);

	}

	protected void createRowDataPackets() throws PEException {
		// create/populate Row Data Packets
		try {
			while (rs.next()) {
				MyRowDataResponse rd = new MyRowDataResponse(dbNative.getResultHandler(), columnSet);
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					rd.addColumnValue(rs.getObject(i));
				}
				addPacket(rd);
			}
		} catch (SQLException sqle) {
			throw new PEException(sqle);
		}
		// add EOF Packet
		addEOFPacket(2, 0);

	}
	
	public Iterator<MyResponseMessage> getListIterator() {
		return resultSetPackets.iterator();
	}

	public int getPacketCount() {
		return resultSetPackets.size();
	}
	
	public long getFieldCount() {
		return fieldCount;
	}

	public void setFieldCount(long fieldCount) {
		this.fieldCount = fieldCount;
	}

	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		MysqlAPIUtils.putLengthCodedLong(cb, fieldCount);
	}

	public void addEOFPacket(int serverStatus, int warningCount) {
		addPacket(new MyEOFPktResponse((short) serverStatus, (short) warningCount));
	}

	@Override
	public MyMessageType getMessageType() {
		return MyMessageType.RESULTSET_RESPONSE;
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		throw new PECodingException("Method not supported for " + this.getClass().getName() );
	}

}
